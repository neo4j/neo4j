package org.neo4j.procedure.impl;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;

final class CompositeConverter implements Function<String,DefaultParameterValue>
{
    private final Iterable<Function<String,DefaultParameterValue>> functions;

    @SafeVarargs
    CompositeConverter( Function<String,DefaultParameterValue>... functions )
    {
        this.functions = List.of( functions );
    }

    @Override
    public DefaultParameterValue apply( String s )
    {
        for ( Iterator<Function<String,DefaultParameterValue>> iterator = functions.iterator(); iterator.hasNext(); )
        {
            Function<String,DefaultParameterValue> function = iterator.next();
            try
            {
                return function.apply( s );
            }
            catch ( IllegalArgumentException invalidConversion )
            {
                if ( !iterator.hasNext() )
                {
                    throw invalidConversion;
                }
            }
        }

        throw new IllegalArgumentException( String.format( "%s is not a valid map expression", s ) );
    }
}
