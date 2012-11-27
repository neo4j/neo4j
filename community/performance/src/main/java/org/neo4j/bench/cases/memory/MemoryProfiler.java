package org.neo4j.bench.cases.memory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.monitoring.runtime.instrumentation.AllocationInstrumenter;
import com.google.monitoring.runtime.instrumentation.AllocationRecorder;
import com.google.monitoring.runtime.instrumentation.Sampler;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.bench.cases.Operation;

public class MemoryProfiler
{

    private static class ProfilingMemorySampler implements Sampler
    {

        private AtomicBoolean isSampling = new AtomicBoolean( false );
        private MemoryProfilingReport report;

        @Override
        public void sampleAllocation( int count, String desc,
                                      Object newObj, long size )
        {
            if(isSampling.get())
            {
                report.recordSample(count, desc, newObj, size);
            }
        }

        public void startProfiling()
        {
            report = new MemoryProfilingReport();
            isSampling.set( true );
        }

        public void stopProfiling()
        {
            isSampling.set( false );
        }

        public MemoryProfilingReport createReport()
        {
            return report;
        }
    }

    private static class MemoryProfilingRunner
    {

        private final ProfilingMemorySampler sampler;

        public MemoryProfilingRunner( )
        {
            sampler = new ProfilingMemorySampler();
            AllocationRecorder.addSampler( sampler );
        }

        public MemoryProfilingReport profileOperation( Operation operaration )
        {
            operaration.setUp();
            try
            {
                sampler.startProfiling();
                operaration.invoke();
                sampler.stopProfiling();
            } finally {
                operaration.tearDown();
            }

            return sampler.createReport();
        }
    }

    //
    // Static methods invoked from the profiling VM we spawn
    //

    public static void main(String ... args)
    {
        if(args.length != 1)
        {
            terminateWithError("No operation class specified");
        }

        try {
            MemoryProfilingRunner runner = new MemoryProfilingRunner();

            MemoryProfilingReport report = runner.profileOperation(createOperation( args[0] ));

            System.out.print( report.serialize() );
            System.out.print( "\0" );
            System.exit( 0 );
        } catch(Throwable e)
        {
            e.printStackTrace( System.err );
        }
    }

    private static Operation createOperation( String testCaseClassName )
    {
        Object testCase = null;
        try
        {
            testCase = MemoryProfiler.class.getClassLoader().loadClass( testCaseClassName ).newInstance();
        }
        catch ( Throwable e )
        {
            terminateWithError( e );
        }

        if(!(testCase instanceof Operation))
        {
            terminateWithError("Test class must be a subclass of Operation");
        }

        return (Operation)testCase;
    }

    private static void terminateWithError( Throwable e )
    {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        e.printStackTrace( printWriter );
        terminateWithError(result.toString());
    }

    private static void terminateWithError( String msg )
    {
        System.out.println("{\"error\":\""+msg+"\"}\0");
        System.exit( 1 );
    }

    //
    // Class implementation, used in the main VM
    //

    public MemoryProfilingReport run(Class<? extends Operation> memoryTestCase)
    {

        String memoryAgentJar = memoryAgentJar();
        Process proc = start( "java",
                "-javaagent:" + memoryAgentJar,
                "-Xmx1G",
                "-cp", createClasspath(),
                MemoryProfiler.class.getName(),
                memoryTestCase.getName() );

        InputStream source = proc.getInputStream();
        InputStream err = proc.getErrorStream();

        StringBuilder out = new StringBuilder( );
        try {

            outerloop: while(true)
            {
                // Read errors
                StringBuilder errOut = new StringBuilder();
                pipeAvailableChars( err, errOut );
                String errors = errOut.toString();
                if ( errors.length() > 0 )
                {
                    System.err.print( errors );
                }

                // Read stdin
                if ( pipeAvailableChars( source, out ) )
                {
                    break outerloop;
                }

                Thread.sleep( 10 );
            }
        } catch(IOException e)
        {
            e.printStackTrace(  );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }

        return MemoryProfilingReport.deserialize( out.toString() );
    }

    private String createClasspath()
    {
        Collection<String> newCP = new ArrayList<String>();
        for ( String pathEntry : System.getProperty( "java.class.path" ).split( File.pathSeparator ) )
        {
            System.out.println(pathEntry);
            newCP.add( pathEntry );
        }

        return StringUtils.join(newCP, File.pathSeparator);
    }

    private boolean pipeAvailableChars( InputStream source, StringBuilder out ) throws IOException
    {
        int available = source.available();
        if ( available != 0 )
        {
            byte[] data = new byte[available /*- ( available % 2 )*/];
            source.read( data );
            ByteBuffer chars = ByteBuffer.wrap( data );
            while ( chars.hasRemaining() )
            {
                char c = (char) chars.get();
                if ( c == '\0')
                {
                    return true;
                } else
                {
                    out.append( c );
                }
            }
        }
        return false;
    }

    private String memoryAgentJar(  )
    {
        AgentJarFactory agentFactory = new AgentJarFactory();
        return agentFactory.createPremainAgentJar( AllocationInstrumenter.class );
    }

    private Process start( String... args )
    {
        ProcessBuilder builder = new ProcessBuilder( args );
        try
        {
            return builder.start();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to start sub process", e );
        }
    }
}
