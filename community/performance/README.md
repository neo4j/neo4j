# Performance testing tools

This project defines a set of modules that can be used for performance tests of various sorts.

The modules included here depend on one another, and are as follows:

## Performance Framework

Defines a general java performance testing framework, based around the
concept of writing Benchmarks. The core benchmark interface simply defines setUp, tearDown
and run methods, where run is allowed to do any sort of profiling it likes, and then return
a set of metrics as result.

The framework contains several specialized implementations of the Benchmark interface, for instance:

 * BenchmarkAdapter - Which defines empty setUp, tearDown and a name method
 * SimpleBenchmark - Which does ops-per-second profiling for you
 * ConcurrentBenchmark - Which helps with running concurrent performance tests
 * MemoryBenchmark - Which performs memory profiling

## Graph Performance Framework

Extends the Performance Framework with graph database specific tooling.

## Performance Dashboard

A stand alone application which picks up Benchmark implementations from classpath, runs them,
and then keeps track of old results. This allows the dashboard to handle things like
tracking regression of specific metrics, as well as presenting a web interface for getting
an overview of the individual benchmarks.
