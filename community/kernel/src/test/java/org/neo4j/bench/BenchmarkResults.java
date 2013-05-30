package org.neo4j.bench;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class BenchmarkResults
{
    private final PrintStream out;
    private final String elapsedTimeUnit;
    private final String durationUnit;

    public BenchmarkResults( File outputResultsFile, String elapsedTimeUnit, String durationUnit ) throws FileNotFoundException
    {
        this.out = new PrintStream( outputResultsFile );
        this.elapsedTimeUnit = elapsedTimeUnit;
        this.durationUnit = durationUnit;
        writeHeader();
    }

    private void writeHeader()
    {
        out.printf( "ElapsedTime_%s\tOperation\tSuccesses\tFailures\tDuration_%s%n", elapsedTimeUnit, durationUnit );
    }

    public void writeResult( long elapsedTime, String operation, int successes, int failures, long duration )
    {
        out.printf("%d\t%s\t%d\t%d\t%d%n", elapsedTime, operation, successes, failures, duration );
    }

    public void close()
    {
        out.close();
    }
}
