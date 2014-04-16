package org.neo4j.server.web;

import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.log.Slf4jLog;

/**
 * Slf4jLog.isDebugEnabled delegates in the end to Logback, and since this method is called a lot and that method
 * is relatively slow, it has a big impact on the overall performance. This subclass fixes that by calling
 * isDebugEnabled
 * on creation, and then caches that.
 */
public class FastSlf4jLog
        extends Slf4jLog
{
    private static boolean debugEnabled;

    public FastSlf4jLog() throws Exception
    {
        this( "org.eclipse.jetty.util.log" );
    }

    public FastSlf4jLog( String name )
    {
        super( name );

        debugEnabled = super.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }

    @Override
    public void debug( String msg, Object... args )
    {
        if ( debugEnabled )
        {
            if ( args != null && args.length == 0 )
            {
                args = null;
            }

            super.debug( msg, args );
        }
    }

    @Override
    public void debug( Throwable thrown )
    {
        if ( debugEnabled )
        {
            super.debug( thrown );
        }
    }

    @Override
    public void debug( String msg, Throwable thrown )
    {
        if ( debugEnabled )
        {
            super.debug( msg, thrown );
        }
    }

    protected Logger newLogger( String fullname )
    {
        return new FastSlf4jLog( fullname );
    }
}
