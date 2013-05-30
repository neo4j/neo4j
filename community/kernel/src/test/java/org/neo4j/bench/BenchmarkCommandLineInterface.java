package org.neo4j.bench;

import java.io.File;
import java.io.PrintStream;

import static java.lang.System.getProperty;
import static javax.xml.bind.DatatypeConverter.parseInt;

public class BenchmarkCommandLineInterface
{
    public int evaluate( String[] args, Describer describer, RunBenchCase benchCase ) throws Exception
    {
        if ( args.length == 1 )
        {
            if ( "describe".equals( args[0] ) )
            {
                describer.describe( System.out );
                return 0;
            }
            if ( "run".equals( args[0] ) )
            {
                return benchCase.run( new BasicParameters(
                        new File( getProperty( "outputResultsFile" ) ),
                        parseInt( getProperty( "totalDuration" ) ) * 1000 ) );
            }
        }
        System.err.println( "Usage: specify 'run' or 'describe'" );
        return 1;
    }

    public interface RunBenchCase
    {
        int run(BasicParameters parameters) throws Exception;

    }
    public interface Describer
    {
        void describe(PrintStream out);

    }
    public static class BasicParameters
    {
        public final File outputResultsFile;

        public final long totalDuration;
        public BasicParameters( File outputResultsFile, long totalDuration )
        {
            this.outputResultsFile = outputResultsFile;
            this.totalDuration = totalDuration;
        }
    }
}
