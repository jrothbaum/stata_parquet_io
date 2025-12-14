package com.parquet.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.arrow.vector.types.FloatingPointPrecision;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import com.stata.sfi.*;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.*;


public class ParquetIO {
    private static RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    
    // Store a callback that Rust can use
    private static long classReference = 0;
    
    // Load your Rust library with exact path
    static {
        String jarPath = ParquetIO.class.getProtectionDomain()
            .getCodeSource().getLocation().getPath();
        String jarDir = new java.io.File(jarPath).getParent();
        String libPath = jarDir + "/pq.plugin";
        
        System.load(libPath);
    }
    
    // Declare the native method (implemented in Rust)
    private static native int stataCall(String[] args);
    
    // This will be called BY RUST to store the class reference
    public static void setClassReference(long ref) {
        classReference = ref;
        //  SFIToolkit.displayln("Class reference stored: " + ref);
    }
    
    // This will be called BY RUST to get the class reference back
    public static long getClassReference() {
        return classReference;
    }
    
    public static int execute(String[] args) {
        //  SFIToolkit.displayln("Hello from execute!");
        return stataCall(args);
    }

    public static void testMethod() {
        SFIToolkit.displayln("testMethod called from Rust!");
        SFIToolkit.displayln("  (from java)");
    }

    public static void assignToStata(
        long schemaPtr, 
        long arrayPtr,
        long offsetRows,
        int numThreads,
        String[] strlColumns,
        String[] variables,
        int[] indices
    ) {
        ArrowSchema arrowSchema = ArrowSchema.wrap(schemaPtr);
        ArrowArray arrowArray = ArrowArray.wrap(arrayPtr);
        
        try {
            // Create mapping from variable name to Stata column index
            Map<String, Integer> varToIndex = new HashMap<>();
            for (int i = 0; i < variables.length; i++) {
                varToIndex.put(variables[i], indices[i]);
            }

            //  SFIToolkit.displayln("Step 1: Importing VectorSchemaRoot");
            
            VectorSchemaRoot root = org.apache.arrow.c.Data.importVectorSchemaRoot(
                allocator, arrowArray, arrowSchema, null
            );
            
            //  SFIToolkit.displayln("Step 2: Getting dimensions");
            
            int numRows = root.getRowCount();
            int numCols = root.getFieldVectors().size();
            
            //  SFIToolkit.displayln("Received DataFrame: " + numRows + " rows, " + numCols + " columns");
            
            // Create set of special columns for fast lookup
            Set<String> strlColumnsSet = new HashSet<>(Arrays.asList(strlColumns));
            //  SFIToolkit.displayln("StrL columns: " + strlColumnsSet);
        
            // Determine batch size
            int batchSize = (int) Math.ceil((double) numRows / numThreads);
            
            //  SFIToolkit.displayln("Processing with " + numThreads + " threads, batch size: " + batchSize);
            
            // Create thread pool
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<?>> futures = new ArrayList<>();
            
            // Submit batch tasks
            for (int batchIdx = 0; batchIdx < numThreads; batchIdx++) {
                int startRow = batchIdx * batchSize;
                int endRow = Math.min(startRow + batchSize, numRows);
                
                if (startRow >= numRows) break;
                
                final int batchId = batchIdx;
                Future<?> future = executor.submit(() -> {
                    processBatch(
                        root, 
                        startRow, 
                        endRow, 
                        numCols, 
                        batchId,
                        varToIndex, 
                        strlColumnsSet,
                        offsetRows
                    );
                });
                futures.add(future);
            }
            
            // Wait for all batches to complete
            for (Future<?> future : futures) {
                future.get();
            }
            
            executor.shutdown();
            
            // Handle strL columns sequentially (not thread-safe)
            //  SFIToolkit.displayln("Step 5: Processing strL columns sequentially");
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                FieldVector vector = root.getVector(colIdx);
                Field field = vector.getField();
                String colName = field.getName();
                
                // Skip if not a strL column
                if (!strlColumnsSet.contains(colName)) {  // Fixed variable name
                    continue;
                }
                
                Integer stataVarIdx = varToIndex.get(colName);
                if (stataVarIdx == null) {
                    continue;
                }
                
                //  SFIToolkit.displayln("Processing strL column: " + colName);
                VarCharVector v = (VarCharVector) vector;
                
                // Loop over all rows for this strL column
                for (int row = 0; row < numRows; row++) {
                    long stataRow = offsetRows + row;
                    if (!v.isNull(row)) {
                        Data.storeStrfFast(stataVarIdx, stataRow, v.getObject(row).toString());  // Use storeStrfFast instead
                    }
                }
            }


            //  SFIToolkit.displayln("Step 5: Closing root");
            root.close();
            
            //  SFIToolkit.displayln("Step 6: Complete");
            
        } catch (Exception e) {
            SFIToolkit.displayln("ERROR in assignToStata: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to receive Arrow data", e);
        }
    }

    private static void processBatch(
        VectorSchemaRoot root, 
        int startRow, 
        int endRow, 
        int numCols, 
        int batchId, 
        Map<String, Integer> varToIndex,
        Set<String> strlColumns, 
        long offsetRows
    ) {
        try {
            //  SFIToolkit.displayln("Batch " + batchId + ": Processing rows " + startRow + " to " + endRow);
            
            // Pre-build column handlers array for this batch (avoid repeated lookups)
            NumericHandler[] numericHandlers = new NumericHandler[numCols];
            StringHandler[] stringHandlers = new StringHandler[numCols];
            
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                FieldVector vector = root.getVector(colIdx);
                Field field = vector.getField();
                String colName = field.getName();
                
                Integer stataVarIdx = varToIndex.get(colName);
                if (stataVarIdx == null) {
                    continue;
                }

                // Skip strL columns - they'll be handled separately
                if (strlColumns.contains(colName)) {
                    continue;
                }
                
                ArrowType.ArrowTypeID typeId = field.getType().getTypeID();
                
                // Check if numeric type
                switch (typeId) {
                    case Int:
                    case FloatingPoint:
                    case Decimal:
                    case Date:
                    case Time:
                    case Timestamp:
                        numericHandlers[colIdx] = createNumericHandler(vector, stataVarIdx, typeId);
                        break;
                        
                    case Utf8:
                    case LargeUtf8:
                    case Utf8View:
                        stringHandlers[colIdx] = createStringHandler(vector, stataVarIdx);
                        break;
                        
                    default:
                        SFIToolkit.displayln("Unsupported type for column " + colName + ": " + typeId);
                }
            }

            // Loop over rows (outer) then columns (inner) - row-oriented
            for (int row = startRow; row < endRow; row++) {
                long stataRow = offsetRows + row;
                
                // Process all numeric columns for this row
                for (int colIdx = 0; colIdx < numCols; colIdx++) {
                    NumericHandler handler = numericHandlers[colIdx];
                    if (handler != null) {
                        handler.store(row, stataRow);
                    }
                }
                
                // Process all string columns for this row
                for (int colIdx = 0; colIdx < numCols; colIdx++) {
                    StringHandler handler = stringHandlers[colIdx];
                    if (handler != null) {
                        handler.store(row, stataRow);
                    }
                }
            }
            

            //  
            //  SFIToolkit.displayln("Batch " + batchId + ": Complete");
            
        } catch (Exception e) {
            SFIToolkit.displayln("ERROR in batch " + batchId + ": " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Batch processing failed", e);
        }

        

    }

    private interface NumericHandler {
        void store(int arrowRow, long stataRow);
    }

    private interface StringHandler {
        void store(int arrowRow, long stataRow);
    }

    private static NumericHandler createNumericHandler(FieldVector vector, int stataVarIdx, 
                                                        ArrowType.ArrowTypeID typeId) {
        switch (typeId) {
            case Int:
                ArrowType.Int intType = (ArrowType.Int) vector.getField().getType();
                int bitWidth = intType.getBitWidth();

                if (bitWidth == 8) {
                    TinyIntVector v = (TinyIntVector) vector;
                    return (arrowRow, stataRow) -> {
                        if (!v.isNull(arrowRow)) {
                            Data.storeNumFast(stataVarIdx, stataRow, (double) v.get(arrowRow));
                        }
                    };
                } else if (bitWidth == 16) {
                    SmallIntVector v = (SmallIntVector) vector;
                    return (arrowRow, stataRow) -> {
                        if (!v.isNull(arrowRow)) {
                            Data.storeNumFast(stataVarIdx, stataRow, (double) v.get(arrowRow));
                        }
                    };
                } else if (bitWidth == 32) {
                    IntVector v = (IntVector) vector;
                    return (arrowRow, stataRow) -> {
                        if (!v.isNull(arrowRow)) {
                            Data.storeNumFast(stataVarIdx, stataRow, (double) v.get(arrowRow));
                        }
                    };
                } else if (bitWidth == 64) {
                    BigIntVector v = (BigIntVector) vector;
                    return (arrowRow, stataRow) -> {
                        if (!v.isNull(arrowRow)) {
                            Data.storeNumFast(stataVarIdx, stataRow, (double) v.get(arrowRow));
                        }
                    };
                }
                return null;
                
            case FloatingPoint:
                ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) vector.getField().getType();
                if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
                    Float8Vector v = (Float8Vector) vector;
                    return (arrowRow, stataRow) -> {
                        if (!v.isNull(arrowRow)) {
                            Data.storeNumFast(stataVarIdx, stataRow, v.get(arrowRow));
                        }
                    };
                } else {
                    Float4Vector v = (Float4Vector) vector;
                    return (arrowRow, stataRow) -> {
                        if (!v.isNull(arrowRow)) {
                            Data.storeNumFast(stataVarIdx, stataRow, (double) v.get(arrowRow));
                        }
                    };
                }
                
            default:
                return null;
        }
    }

    private static StringHandler createStringHandler(FieldVector vector, int stataVarIdx) {
        ArrowType.ArrowTypeID typeId = vector.getField().getType().getTypeID();
        
        if (typeId == ArrowType.ArrowTypeID.Utf8View) {
            ViewVarCharVector v = (ViewVarCharVector) vector;
            return (arrowRow, stataRow) -> {
                if (!v.isNull(arrowRow)) {
                    Data.storeStrfFast(stataVarIdx, stataRow, v.getObject(arrowRow).toString());
                }
            };
        } else {  // Utf8 or LargeUtf8
            VarCharVector v = (VarCharVector) vector;
            return (arrowRow, stataRow) -> {
                if (!v.isNull(arrowRow)) {
                    Data.storeStrfFast(stataVarIdx, stataRow, v.getObject(arrowRow).toString());
                }
            };
        }
    }
    
    private static native void releaseArrowSchema(long schemaPtr);
    private static native void releaseArrowArray(long arrayPtr);
}
