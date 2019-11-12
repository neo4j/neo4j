package org.neo4j.procedure.impl;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

final class CompositeConverter implements Function<String,DefaultParameterValue>
{
    private final Neo4jTypes.AnyType upCastTo;
    private final Iterable<Function<String,DefaultParameterValue>> functions;

    @SafeVarargs
    CompositeConverter( Neo4jTypes.AnyType upCastTo, Function<String,DefaultParameterValue>... functions )
    {
        this.upCastTo = upCastTo;
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
                return function.apply( s ).castAs( upCastTo );
            }
            catch ( IllegalArgumentException invalidConversion )
            {
                if ( !iterator.hasNext() )
                {
                    throw invalidConversion;
                }
            }
        }

        throw new IllegalArgumentException( String.format( "%s is not a valid default value expression", s ) );
    }
}
