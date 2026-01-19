package com.parquet.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    
    // Persistent thread pool
    private static ExecutorService executor = null;
    private static int currentThreadCount = 0;


    private static final String LIB_NAME = "pq.plugin";
    // Use a volatile flag to track the load state.
    // Start as false, as the library must be loaded before use.
    private static volatile boolean librarySuccessfullyLoaded = false;
    
    // Store a callback that Rust can use
    private static long classReference = 0;
    static {
        try {
            String jarPath = ParquetIO.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath();
            String jarDir = new java.io.File(jarPath).getParent();
            String libPath = jarDir + "/" + LIB_NAME;
            
            System.load(libPath);
            
        } catch (UnsatisfiedLinkError e) {
            if (!e.getMessage().contains("already loaded")) {
                throw new RuntimeException("Failed to load library", e);
            }
        }
    }
    

    // This will be called BY RUST to store the class reference
    public static void setClassReference(long ref) {
        classReference = ref;
    }
    
    // This will be called BY RUST to get the class reference back
    public static long getClassReference() {
        return classReference;
    }
    
    // Declare the native method (implemented in Rust)
    private static native int stataCall(String[] args);

    
    public static int execute(String[] args) {
        try {
            return stataCall(args);
        } catch (UnsatisfiedLinkError e) {
            SFIToolkit.displayln();
            SFIToolkit.error("═══════════════════════════════════════════════════");
            SFIToolkit.displayln();
            SFIToolkit.error("ERROR: pq plugin cannot be used after 'clear all'");
            SFIToolkit.displayln();
            SFIToolkit.error("Please restart Stata to continue");
            SFIToolkit.displayln();
            SFIToolkit.error("Instead of 'clear all', you can use 'pq clear'");
            SFIToolkit.displayln();
            SFIToolkit.error("which is equivalent to:");
            SFIToolkit.displayln();
            SFIToolkit.error("      clear");
            SFIToolkit.displayln();
            SFIToolkit.displayln();
            SFIToolkit.error("      macro drop _all");
            SFIToolkit.displayln();
            SFIToolkit.error("      scalar drop _all");
            SFIToolkit.displayln();
            SFIToolkit.error("      matrix drop _all");
            SFIToolkit.displayln();
            SFIToolkit.error("      timer clear");
            SFIToolkit.displayln();
            SFIToolkit.error("═══════════════════════════════════════════════════");
            SFIToolkit.displayln();
            throw new RuntimeException("Restart Stata required", e);
        }
    }

    public static void testMethod() {
        SFIToolkit.displayln("testMethod called from Rust!");
        SFIToolkit.displayln("  (from java)");
    }
    
    // Get or create executor with specified thread count
    private static synchronized ExecutorService getExecutor(int numThreads) {
        if (executor == null || currentThreadCount != numThreads) {
            if (executor != null) {
                executor.shutdown();
                try {
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                }
            }
            executor = Executors.newFixedThreadPool(numThreads);
            currentThreadCount = numThreads;
        }
        return executor;
    }
    
    // Call from Rust after all batches processed
    public static synchronized void shutdown() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
            executor = null;
            currentThreadCount = 0;
        }
    }

    // Column metadata for efficient processing (used in assignToStata - reading Arrow into Stata)
    private static class ColumnInfo {
        final int stataVarIdx;
        final FieldVector vector;
        final ArrowType.ArrowTypeID typeId;
        final boolean isStrL;
        final int bitWidth; // for Int types
        final FloatingPointPrecision precision; // for Float types
        
        ColumnInfo(int stataIdx, FieldVector vec, boolean strL) {
            this.stataVarIdx = stataIdx;
            this.vector = vec;
            this.isStrL = strL;
            this.typeId = vec.getField().getType().getTypeID();
            
            if (typeId == ArrowType.ArrowTypeID.Int) {
                this.bitWidth = ((ArrowType.Int) vec.getField().getType()).getBitWidth();
                this.precision = null;
            } else if (typeId == ArrowType.ArrowTypeID.FloatingPoint) {
                this.bitWidth = 0;
                this.precision = ((ArrowType.FloatingPoint) vec.getField().getType()).getPrecision();
            } else {
                this.bitWidth = 0;
                this.precision = null;
            }
        }
    }

    // ========================================================================
    // IMPORT: Arrow data to Stata (reading parquet into Stata)
    // ========================================================================

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
            
            VectorSchemaRoot root = org.apache.arrow.c.Data.importVectorSchemaRoot(
                allocator, arrowArray, arrowSchema, null
            );
            
            int numRows = root.getRowCount();
            int numCols = root.getFieldVectors().size();
            
            // Create set of special columns for fast lookup during building
            Set<String> strlColumnsSet = new HashSet<>(Arrays.asList(strlColumns));
            
            // DYNAMIC ROW WAVE SIZING based on column count
            int targetCacheSizeBytes = 32 * 1024 * 1024;  // 32 MB L3 cache target
            int bytesPerRow = numCols * 8;  // Assuming numeric columns
            int rowsPerWave = targetCacheSizeBytes / bytesPerRow;

            // Not too few, though
            rowsPerWave = Math.max(16_000, Math.min(rowsPerWave, numRows));
            
            // Build column info ONCE - no more HashMap/Set lookups in hot path
            List<ColumnInfo> regularColumns = new ArrayList<>(numCols);
            List<ColumnInfo> strlColumnsList = new ArrayList<>(numCols);
            
            // Build the column lists
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                FieldVector vector = root.getVector(colIdx);
                Field field = vector.getField();
                String colName = field.getName();
                
                Integer stataVarIdx = varToIndex.get(colName);
                if (stataVarIdx == null) {
                    continue;
                }
                
                boolean isStrL = strlColumnsSet.contains(colName);
                ColumnInfo info = new ColumnInfo(stataVarIdx, vector, isStrL);
                
                if (isStrL) {
                    strlColumnsList.add(info);
                } else {
                    regularColumns.add(info);
                }
            }
            
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            
            for (int waveStartRow = 0; waveStartRow < numRows; waveStartRow += rowsPerWave) {
                int waveEndRow = Math.min(waveStartRow + rowsPerWave, numRows);
                int waveNumRows = waveEndRow - waveStartRow;
                
                int batchSize = (int) Math.ceil((double) waveNumRows / numThreads);
                List<Future<?>> futures = new ArrayList<>();
                
                for (int batchIdx = 0; batchIdx < numThreads; batchIdx++) {
                    int batchStartRow = waveStartRow + (batchIdx * batchSize);
                    int batchEndRow = Math.min(batchStartRow + batchSize, waveEndRow);
                    
                    if (batchStartRow >= waveEndRow) break;
                    
                    Future<?> future = executor.submit(() -> {
                        processBatch(
                            regularColumns,  // Pass the column list
                            batchStartRow, 
                            batchEndRow,
                            offsetRows
                        );
                    });
                    futures.add(future);
                }
                
                for (Future<?> future : futures) {
                    future.get();
                }
                futures.clear();
            }
            
            executor.shutdown();
            
            // Process strL columns sequentially (not thread-safe)
            for (ColumnInfo info : strlColumnsList) {
                VarCharVector v = (VarCharVector) info.vector;
                int stataVarIdx = info.stataVarIdx;
                
                for (int row = 0; row < numRows; row++) {
                    if (!v.isNull(row)) {
                        Data.storeStrfFast(stataVarIdx, offsetRows + row, v.getObject(row).toString());
                    }
                }
            }
            
            root.close();
            
        } catch (Exception e) {
            SFIToolkit.displayln("ERROR in assignToStata: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to receive Arrow data", e);
        }
    }

    // Keep your type-specific processBatch() as is:
    private static void processBatch(
        List<ColumnInfo> columns,
        int startRow,
        int endRow,
        long offsetRows
    ) {
        try {
            for (ColumnInfo col : columns) {
                // Dispatch to type-specific method once per column
                switch (col.typeId) {
                    case Int:
                        processIntColumn(col, startRow, endRow, offsetRows);
                        break;
                    case FloatingPoint:
                        processFloatColumn(col, startRow, endRow, offsetRows);
                        break;
                    case Utf8:
                    case LargeUtf8:
                        processUtf8Column(col, startRow, endRow, offsetRows);
                        break;
                    case Utf8View:
                        processUtf8ViewColumn(col, startRow, endRow, offsetRows);
                        break;
                    default:
                        // Handle unsupported types
                }
            }
        } catch (Exception e) {
            SFIToolkit.displayln("ERROR in processBatch: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Batch processing failed", e);
        }
    }

    private static void processIntColumn(ColumnInfo col, int startRow, int endRow, long offsetRows) {
        if (col.bitWidth == 8) {
            TinyIntVector v = (TinyIntVector) col.vector;
            for (int row = startRow; row < endRow; row++) {
                if (!v.isNull(row)) {
                    Data.storeNumFast(col.stataVarIdx, offsetRows + row, (double) v.get(row));
                }
            }
        } else if (col.bitWidth == 16) {
            SmallIntVector v = (SmallIntVector) col.vector;
            for (int row = startRow; row < endRow; row++) {
                if (!v.isNull(row)) {
                    Data.storeNumFast(col.stataVarIdx, offsetRows + row, (double) v.get(row));
                }
            }
        } else if (col.bitWidth == 32) {
            IntVector v = (IntVector) col.vector;
            for (int row = startRow; row < endRow; row++) {
                if (!v.isNull(row)) {
                    Data.storeNumFast(col.stataVarIdx, offsetRows + row, (double) v.get(row));
                }
            }
        } else if (col.bitWidth == 64) {
            BigIntVector v = (BigIntVector) col.vector;
            for (int row = startRow; row < endRow; row++) {
                if (!v.isNull(row)) {
                    Data.storeNumFast(col.stataVarIdx, offsetRows + row, (double) v.get(row));
                }
            }
        }
    }

    private static void processFloatColumn(ColumnInfo col, int startRow, int endRow, long offsetRows) {
        if (col.precision == FloatingPointPrecision.DOUBLE) {
            Float8Vector v = (Float8Vector) col.vector;
            for (int row = startRow; row < endRow; row++) {
                if (!v.isNull(row)) {
                    Data.storeNumFast(col.stataVarIdx, offsetRows + row, v.get(row));
                }
            }
        } else {
            Float4Vector v = (Float4Vector) col.vector;
            for (int row = startRow; row < endRow; row++) {
                if (!v.isNull(row)) {
                    Data.storeNumFast(col.stataVarIdx, offsetRows + row, (double) v.get(row));
                }
            }
        }
    }

    private static void processUtf8Column(ColumnInfo col, int startRow, int endRow, long offsetRows) {
        VarCharVector v = (VarCharVector) col.vector;
        for (int row = startRow; row < endRow; row++) {
            if (!v.isNull(row)) {
                Data.storeStrfFast(col.stataVarIdx, offsetRows + row, v.getObject(row).toString());
            }
        }
    }

    private static void processUtf8ViewColumn(ColumnInfo col, int startRow, int endRow, long offsetRows) {
        ViewVarCharVector v = (ViewVarCharVector) col.vector;
        for (int row = startRow; row < endRow; row++) {
            if (!v.isNull(row)) {
                Data.storeStrfFast(col.stataVarIdx, offsetRows + row, v.getObject(row).toString());
            }
        }
    }
    
    private static native void releaseArrowSchema(long schemaPtr);
    private static native void releaseArrowArray(long arrayPtr);



    // ========================================================================
    // EXPORT: Stata data to Arrow format (writing Stata to parquet)
    // ========================================================================

    /**
     * Pre-scan strL columns to detect if any contain binary data.
     * Returns a Set of variable names that contain binary data.
     */
    private static Set<String> prescanStrlForBinary(
        String[] strlColumns,
        long totalRows
    ) {
        Set<String> binaryColumns = new HashSet<>();
        
        for (String varName : strlColumns) {
            int varIdx = Data.getVarIndex(varName);
            
            for (long row = 1; row <= totalRows; row++) {
                try {
                    // Try to read as string - if it fails, it's binary
                    Data.getStr(varIdx, row);
                } catch (Exception e) {
                    // Found binary data - mark this column and move to next
                    binaryColumns.add(varName);
                    break;
                }
            }
        }
        
        return binaryColumns;
    }

    /**
     * Export Stata data as an Arrow RecordBatch via FFI.
     * Returns array of two longs: [schemaPtr, arrayPtr]
     * Caller (Rust) is responsible for managing these pointers.
     * 
     * @param variables Array of variable names to export
     * @param startRow Starting row (0-indexed)
     * @param batchSize Number of rows to export
     * @param numThreads Number of threads for parallel processing
     * @param strlColumns Array of variable names that are strL type
     * @return long[2] containing [schemaPtr, arrayPtr]
     */
    public static long[] exportFromStata(
        String[] variables,
        long startRow,
        int batchSize,
        int numThreads,
        String[] strlColumns
    ) {
        VectorSchemaRoot root = null;
        
        try {
            // Pre-scan strL columns to detect binary
            long totalRows = Data.getObsCount();
            Set<String> binaryStrlColumns = prescanStrlForBinary(strlColumns, totalRows);
            
            // Create set of strL columns for fast lookup
            Set<String> strlColumnsSet = new HashSet<>(Arrays.asList(strlColumns));
        
            // Build schema from Stata variable types
            List<Field> fields = new ArrayList<>(variables.length);
            
            for (String varName : variables) {
                int varIdx = Data.getVarIndex(varName);
                if (varIdx < 0) {
                    throw new IllegalArgumentException("Variable not found: " + varName);
                }
                
                int varType = Data.getType(varIdx);
                boolean isBinaryStrL = binaryStrlColumns.contains(varName);
                Field field = createFieldFromStataType(varName, varType, varIdx, isBinaryStrL);
                fields.add(field);
            }
            
            Schema schema = new Schema(fields);
            root = VectorSchemaRoot.create(schema, allocator);
            
            // Allocate vectors for the batch
            root.setRowCount(batchSize);
            
            // Build column handlers - separate regular and strL columns
            List<ExportHandler> regularHandlers = new ArrayList<>(variables.length);
            List<ExportHandler> strlHandlers = new ArrayList<>(variables.length);
            
            for (int colIdx = 0; colIdx < variables.length; colIdx++) {
                String varName = variables[colIdx];
                int varIdx = Data.getVarIndex(varName);
                int varType = Data.getType(varIdx);
                FieldVector vector = root.getVector(colIdx);
                
                boolean isStrL = strlColumnsSet.contains(varName);
                boolean isBinaryStrL = binaryStrlColumns.contains(varName);
                
                // Pre-allocate string vectors for parallel access
                if (Data.isVarTypeString(varIdx) && !isStrL) {
                    ViewVarCharVector v = (ViewVarCharVector) vector;
                    v.allocateNew();
                }
                
                ExportHandler handler = createExportHandler(varIdx, vector, varType, isStrL, isBinaryStrL);
                
                if (isStrL) {
                    strlHandlers.add(handler);
                } else {
                    regularHandlers.add(handler);
                }
            }
            
            // Process regular columns in parallel
            int threadBatchSize = (int) Math.ceil((double) batchSize / numThreads);
            ExecutorService exec = getExecutor(numThreads);
            List<Future<?>> futures = new ArrayList<>(numThreads);
            
            for (int batchIdx = 0; batchIdx < numThreads; batchIdx++) {
                int batchStartRow = batchIdx * threadBatchSize;
                int batchEndRow = Math.min(batchStartRow + threadBatchSize, batchSize);
                if (batchStartRow >= batchSize) break;
                
                final int start = batchStartRow;
                final int end = batchEndRow;
                Future<?> future = exec.submit(() -> {
                    exportBatch(regularHandlers, start, end, startRow);
                });
                futures.add(future);
            }
            
            // Wait for all parallel batches to complete
            for (Future<?> future : futures) {
                future.get();
            }
            
            // Process strL columns sequentially (not thread-safe)
            for (ExportHandler handler : strlHandlers) {
                handler.allocate(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    long stataRow = startRow + i + 1;  // +1 for Stata's 1-based indexing
                    if (stataRow > Data.getObsCount()) break;
                    handler.exportValue(i, stataRow);
                }
                handler.finalize(batchSize);
            }
            
            // Finalize all regular column vectors
            for (ExportHandler handler : regularHandlers) {
                handler.finalize(batchSize);
            }
            
            // Export to C Data Interface
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
            
            org.apache.arrow.c.Data.exportVectorSchemaRoot(
                allocator, root, null, arrowArray, arrowSchema
            );
            
            // Return pointers as array
            long[] pointers = new long[2];
            pointers[0] = arrowSchema.memoryAddress();
            pointers[1] = arrowArray.memoryAddress();
            
            return pointers;
            
        } catch (Exception e) {
            SFIToolkit.displayln("ERROR in exportFromStata: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to export Arrow data", e);
        } finally {
            // Note: We don't close root here because Rust needs to consume the data
            // Rust must call releaseArrowPointers when done
        }
    }

    /**
     * Process a batch of rows in parallel, reading from Stata and writing to Arrow vectors.
     */
    private static void exportBatch(
        List<ExportHandler> handlers,
        int startRow,
        int endRow,
        long offsetRows
    ) {
        try {
            for (int row = startRow; row < endRow; row++) {
                long stataRow = offsetRows + row + 1;  // +1 for Stata's 1-based indexing
                if (stataRow > Data.getObsCount()) break;
                
                for (ExportHandler handler : handlers) {
                    handler.exportValue(row, stataRow);
                }
            }
            
        } catch (Exception e) {
            SFIToolkit.displayln("ERROR in exportBatch: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Export batch processing failed", e);
        }
    }

    // Base interface for export handlers
    private interface ExportHandler {
        void allocate(int batchSize);
        void exportValue(int arrowRow, long stataRow);
        void finalize(int batchSize);
    }


    // Factory method to create appropriate handler
    private static ExportHandler createExportHandler(
        int stataVarIdx,
        FieldVector vector,
        int stataVarType,
        boolean isStrL,
        boolean isBinaryStrL
    ) {
        // String types
        if (Data.isVarTypeString(stataVarIdx)) {
            if (isStrL && isBinaryStrL) {
                return new BinaryStrlExportHandler(stataVarIdx, (VarBinaryVector) vector);
            } else if (isStrL) {
                return new StringLExportHandler(stataVarIdx, (ViewVarCharVector) vector);
            } else {
                return new StringExportHandler(stataVarIdx, (ViewVarCharVector) vector);
            }
        }
        
        // Numeric types - use Data constants for comparison
        if (stataVarType == Data.TYPE_BYTE) {
            return new Int8ExportHandler(stataVarIdx, (TinyIntVector) vector);
        } else if (stataVarType == Data.TYPE_INT) {
            return new Int16ExportHandler(stataVarIdx, (SmallIntVector) vector);
        } else if (stataVarType == Data.TYPE_LONG) {
            return new Int32ExportHandler(stataVarIdx, (IntVector) vector);
        } else if (stataVarType == Data.TYPE_FLOAT) {
            return new Float32ExportHandler(stataVarIdx, (Float4Vector) vector);
        } else if (stataVarType == Data.TYPE_DOUBLE) {
            return new Float64ExportHandler(stataVarIdx, (Float8Vector) vector);
        }
        
        throw new RuntimeException("Unsupported Stata type: " + stataVarType);
    }

    // Concrete handler implementations
    private static class StringExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final ViewVarCharVector vector;
        
        StringExportHandler(int idx, ViewVarCharVector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew();
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            String value = Data.getStrf(stataVarIdx, stataRow);
            if (value != null && !value.isEmpty()) {
                vector.setSafe(arrowRow, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    private static class StringLExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final ViewVarCharVector vector;
        
        StringLExportHandler(int idx, ViewVarCharVector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew();
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            String value = Data.getStr(stataVarIdx, stataRow);
            if (value != null && !value.isEmpty()) {
                vector.setSafe(arrowRow, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    private static class BinaryStrlExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final VarBinaryVector vector;
        // Reuse buffers to reduce allocations
        private final byte[] buffer = new byte[8192];
        
        BinaryStrlExportHandler(int idx, VarBinaryVector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew(batchSize);
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            try (StrLConnector connector = new StrLConnector(stataVarIdx, stataRow)) {
                // Check if binary - this is fast
                if (!connector.isBinary()) {
                    // Not binary - read as string (fast path)
                    String value = Data.getStr(stataVarIdx, stataRow);
                    if (value != null && !value.isEmpty()) {
                        vector.setSafe(arrowRow, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    } else {
                        vector.setNull(arrowRow);
                    }
                    return;
                }
                
                // Binary data - slow path
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                int bytesRead;
                
                while ((bytesRead = Data.readBytes(connector, buffer)) > 0) {
                    baos.write(buffer, 0, bytesRead);
                }
                
                byte[] bytes = baos.toByteArray();
                if (bytes.length > 0) {
                    vector.setSafe(arrowRow, bytes);
                } else {
                    vector.setNull(arrowRow);
                }
                
            } catch (Exception e) {
                // Log the error for debugging
                SFIToolkit.displayln("Warning: Failed to read binary strL at row " + stataRow + ": " + e.getMessage());
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }


    private static class Int8ExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final TinyIntVector vector;
        
        Int8ExportHandler(int idx, TinyIntVector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew(batchSize);
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            double value = Data.getNum(stataVarIdx, stataRow);
            if (!Missing.isMissing(value)) {
                vector.setSafe(arrowRow, (byte) value);
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    private static class Int16ExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final SmallIntVector vector;
        
        Int16ExportHandler(int idx, SmallIntVector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew(batchSize);
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            double value = Data.getNum(stataVarIdx, stataRow);
            if (!Missing.isMissing(value)) {
                vector.setSafe(arrowRow, (short) value);
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    private static class Int32ExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final IntVector vector;
        
        Int32ExportHandler(int idx, IntVector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew(batchSize);
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            double value = Data.getNum(stataVarIdx, stataRow);
            if (!Missing.isMissing(value)) {
                vector.setSafe(arrowRow, (int) value);
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    private static class Int64ExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final BigIntVector vector;
        
        Int64ExportHandler(int idx, BigIntVector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew(batchSize);
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            double value = Data.getNum(stataVarIdx, stataRow);
            if (!Missing.isMissing(value)) {
                vector.setSafe(arrowRow, (long) value);
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    private static class Float32ExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final Float4Vector vector;
        
        Float32ExportHandler(int idx, Float4Vector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew(batchSize);
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            double value = Data.getNum(stataVarIdx, stataRow);
            if (!Missing.isMissing(value)) {
                vector.setSafe(arrowRow, (float) value);
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    private static class Float64ExportHandler implements ExportHandler {
        private final int stataVarIdx;
        private final Float8Vector vector;
        
        Float64ExportHandler(int idx, Float8Vector vec) {
            this.stataVarIdx = idx;
            this.vector = vec;
        }
        
        @Override
        public void allocate(int batchSize) {
            vector.allocateNew(batchSize);
        }
        
        @Override
        public void exportValue(int arrowRow, long stataRow) {
            double value = Data.getNum(stataVarIdx, stataRow);
            if (!Missing.isMissing(value)) {
                vector.setSafe(arrowRow, value);
            } else {
                vector.setNull(arrowRow);
            }
        }
        
        @Override
        public void finalize(int batchSize) {
            vector.setValueCount(batchSize);
        }
    }

    /**
     * Create an Arrow Field based on Stata variable type.
     */
    private static Field createFieldFromStataType(String varName, int varType, int varIdx, boolean isBinaryStrl) {
        ArrowType arrowType;
        
        if (Data.isVarTypeString(varIdx)) {
            if (varType == Data.TYPE_STRL && isBinaryStrl) {
                // Binary strL
                arrowType = new ArrowType.Binary();
            } else {
                // String types (str1-str2045 or string strL)
                arrowType = new ArrowType.Utf8View();
            }
        } else if (varType == Data.TYPE_DOUBLE) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (varType == Data.TYPE_FLOAT) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (varType == Data.TYPE_LONG) {
            arrowType = new ArrowType.Int(32, true);
        } else if (varType == Data.TYPE_INT) {
            arrowType = new ArrowType.Int(16, true);
        } else if (varType == Data.TYPE_BYTE) {
            arrowType = new ArrowType.Int(8, true);
        } else {
            // Default to double for unknown types
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        
        return new Field(varName, FieldType.nullable(arrowType), null);
    }

    /**
     * Release resources - called from Rust after consuming the data
     */
    public static void releaseArrowPointers(long schemaPtr, long arrayPtr) {
        try {
            if (schemaPtr != 0) {
                ArrowSchema schema = ArrowSchema.wrap(schemaPtr);
                schema.close();
            }
            if (arrayPtr != 0) {
                ArrowArray array = ArrowArray.wrap(arrayPtr);
                array.close();
            }
        } catch (Exception e) {
            SFIToolkit.displayln("ERROR releasing Arrow pointers: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
