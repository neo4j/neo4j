package org.neo4j.server.enterprise.functional;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

import org.jboss.netty.channel.ChannelException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.neo4j.helpers.Settings.osIsWindows;

public class DumpPortListenerOnNettyBindFailure implements TestRule
{
    private final PrintWriter out;

    public DumpPortListenerOnNettyBindFailure()
    {
        this(System.err);
    }

    public DumpPortListenerOnNettyBindFailure( OutputStream out )
    {
        this( new PrintWriter( out, true ) );
    }

    public DumpPortListenerOnNettyBindFailure( Writer out )
    {
        this.out = out instanceof PrintWriter ? (PrintWriter) out : new PrintWriter( out, true );
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        if ( osIsWindows() )
        {
            return base; // we don't have 'lsof' on windows...
        }
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    base.evaluate();
                }
                catch ( Throwable failure )
                {
                    for ( Throwable cause = failure; cause != null; cause = failure.getCause() )
                    {
                        if ( cause instanceof ChannelException )
                        {
                            String message = cause.getMessage();
                            String portString = message.substring( message.lastIndexOf( ':' ) + 1 );
                            int port;
                            try
                            {
                                port = Integer.parseInt( portString );
                            }
                            catch ( Exception e )
                            {
                                continue;
                            }
                            dumpListenersOn( port );
                        }
                    }
                    throw failure;
                }
            }
        };
    }

    public void dumpListenersOn( int port )
    {
        try
        {
            Process lsof = new ProcessBuilder( "lsof", "-i:" + port ).redirectErrorStream( true ).start();
            lsof.waitFor();
            out.println( readAll( lsof.getInputStream() ) );
        }
        catch ( Exception e )
        {
            out.println( "Could not determine the process listening on :" + port );
        }
    }

    private static String readAll( InputStream stream ) throws IOException
    {
        StringBuilder message = new StringBuilder();
        BufferedReader err = new BufferedReader( new InputStreamReader( stream ) );
        for ( String line; null != (line = err.readLine()); )
        {
            message.append( line ).append( '\n' );
        }
        err.close();
        return message.toString();
    }
}
