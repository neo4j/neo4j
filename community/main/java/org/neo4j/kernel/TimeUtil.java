package org.neo4j.kernel;

import java.util.concurrent.TimeUnit;

public abstract class TimeUtil
{
    public static long parseTimeMillis( String timeWithOrWithoutUnit )
    {
        int unitIndex = -1;
        for ( int i = 0; i < timeWithOrWithoutUnit.length(); i++ )
        {
            char ch = timeWithOrWithoutUnit.charAt( i );
            if ( !Character.isDigit( ch ) )
            {
                unitIndex = i;
                break;
            }
        }
        if ( unitIndex == -1 )
        {
            return Integer.parseInt( timeWithOrWithoutUnit );
        }
        else
        {
            int amount = Integer.parseInt( timeWithOrWithoutUnit.substring( 0, unitIndex ) );
            String unit = timeWithOrWithoutUnit.substring( unitIndex ).toLowerCase();
            TimeUnit timeUnit = null;
            if ( unit.equals( "ms" ) )
            {
                timeUnit = TimeUnit.MILLISECONDS;
            }
            else if ( unit.equals( "s" ) )
            {
                timeUnit = TimeUnit.SECONDS;
            }
            else if ( unit.equals( "m" ) )
            {
                timeUnit = TimeUnit.MINUTES;
            }
            else
            {
                throw new RuntimeException( "Unrecognized unit " + unit );
            }
            return timeUnit.toMillis( amount );
        }
    }
}
