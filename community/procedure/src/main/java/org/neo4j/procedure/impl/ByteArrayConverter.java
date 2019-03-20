/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.impl;

import java.util.List;
import java.util.function.Function;

import org.neo4j.cypher.internal.evaluator.EvaluationException;
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntByteArray;

public class ByteArrayConverter implements Function<String,DefaultParameterValue>
{
    private final ExpressionEvaluator evaluator;

    ByteArrayConverter( ExpressionEvaluator evaluator )
    {
        this.evaluator = evaluator;
    }

    @Override
    public DefaultParameterValue apply( String s )
    {
        try
        {
            List<?> evaluate = evaluator.evaluate( s, List.class );
            if ( evaluate == null )
            {
                return ntByteArray( null );
            }
            else
            {
                byte[] bytes = new byte[evaluate.size()];
                for ( int i = 0; i < bytes.length; i++ )
                {
                    bytes[i] = safeGetByte( evaluate.get( i ) );
                }
                return ntByteArray( bytes );
            }
        }
        catch ( EvaluationException e )
        {
            throw new IllegalArgumentException( format( "%s is not a valid list expression", s ), e );
        }
    }

    private byte safeGetByte( Object value )
    {
        if ( value instanceof Number )
        {
            return ((Number) value).byteValue();
        }
        else
        {
            throw new IllegalArgumentException( format( "Expected list to contain numbers but got %s", value ) );
        }
    }
}
