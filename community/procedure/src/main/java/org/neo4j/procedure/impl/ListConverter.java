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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Function;

import org.neo4j.cypher.internal.evaluator.EvaluationException;
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntList;

public class ListConverter implements Function<String,DefaultParameterValue>
{
    private final Neo4jTypes.AnyType neoType;
    private final ExpressionEvaluator evaluator;
    private final Type type;

    ListConverter( Type type, Neo4jTypes.AnyType neoType, ExpressionEvaluator evaluator )
    {
        this.type = type;
        this.neoType = neoType;
        this.evaluator = evaluator;
    }

    @Override
    public DefaultParameterValue apply( String s )
    {
        try
        {
            List<?> list = evaluator.evaluate( s, List.class );
            typeCheck( list, type );
            return ntList( list, neoType );
        }
        catch ( EvaluationException e )
        {
            throw new IllegalArgumentException( String.format( "%s is not a valid list expression", s ), e );
        }
    }

    private void typeCheck( List<?> list, Type innerType )
    {
        if ( list == null )
        {
            return;
        }
        for ( Object obj : list )
        {
            if ( obj != null )
            {
                if ( innerType instanceof Class<?> )
                {
                    Class<?> clazz = (Class<?>) innerType;
                    if ( !clazz.isAssignableFrom( obj.getClass() ) )
                    {
                        throw new IllegalArgumentException(
                                String.format( "Expects a list of %s but got a list of %s", clazz.getSimpleName(),
                                        obj.getClass().getSimpleName() ) );
                    }
                }
                else if ( List.class.isAssignableFrom( obj.getClass() ) && innerType instanceof ParameterizedType )
                {
                    typeCheck( (List<?>) obj, ((ParameterizedType) innerType).getActualTypeArguments()[0] );
                }
            }
        }
    }
}
