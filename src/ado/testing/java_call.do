capture program drop tryjava
program tryjava
    version 17
    di "HI"
    javacall ParquetIO sayhello, jar(hello.jar)
    di "BYE"
end
tryjava