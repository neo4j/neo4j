package org.neo4j.kernel;

import java.util.concurrent.TimeUnit;

public abstract class TimeUtil
{
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    
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
            return DEFAULT_TIME_UNIT.toMillis( Integer.parseInt( timeWithOrWithoutUnit ) );
        }
        else
        {
            int amount = Integer.parseInt( timeWithOrWithoutUnit.substring( 0, unitIndex ) );
            String unit = timeWithOrWithoutUnit.substring( unitIndex ).toLowerCase();
            TimeUnit timeUnit = null;
            int multiplyFactor = 1;
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
                // This is only for having to rely on 1.6
                timeUnit = TimeUnit.SECONDS;
                multiplyFactor = 60;
            }
            else
            {
                throw new RuntimeException( "Unrecognized unit " + unit );
            }
            return timeUnit.toMillis( amount*multiplyFactor );
        }
    }
}
