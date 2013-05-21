package org.neo4j.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.Clock;

public class InputStreamAwaiter
{
    private final InputStream input;
    private final byte[] bytes = new byte[1024];
    private final Clock clock;

    public InputStreamAwaiter( InputStream input )
    {
        this( Clock.SYSTEM, input );
    }

    public InputStreamAwaiter( Clock clock, InputStream input )
    {
        this.clock = clock;
        this.input = input;
    }

    public void awaitLine( String expectedLine, long timeout, TimeUnit unit ) throws IOException,
            TimeoutException, InterruptedException
    {
        long deadline = clock.currentTimeMillis() + unit.toMillis( timeout );
        StringBuilder buffer = new StringBuilder();
        do
        {
            while ( input.available() > 0 )
            {
                buffer.append( new String( bytes, 0, input.read( bytes ) ) );
            }

            String[] lines = buffer.toString().split( "\n" );
            for ( String line : lines )
            {
                if ( expectedLine.equals( line ) )
                {
                    return;
                }
            }

            Thread.sleep( 10 );
        }
        while ( clock.currentTimeMillis() < deadline );

        throw new TimeoutException( "Timed out waiting to read line: [" + expectedLine + "]. Seen input:\n\t"
                + buffer.toString().replaceAll( "\n", "\n\t" ) );
    }
}
