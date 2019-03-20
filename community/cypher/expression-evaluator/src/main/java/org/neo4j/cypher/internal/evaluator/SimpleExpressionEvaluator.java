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
package org.neo4j.cypher.internal.evaluator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

class SimpleExpressionEvaluator implements ExpressionEvaluator
{
    private InternalExpressionEvaluator evaluator = new SimpleInternalExpressionEvaluator();

    @Override
    public <T> T evaluate( String expression, Class<T> type ) throws EvaluationException
    {
        if ( expression == null )
        {
            throw new EvaluationException( "Cannot evaluate null as an expression " );
        }
        if ( type == null )
        {
            throw new EvaluationException( "Cannot evaluate to type null" );
        }

        return cast( map( evaluator.evaluate( expression ) ), type );
    }

    private <T> T cast( Object value, Class<T> type ) throws EvaluationException
    {
        try
        {
            return type.cast( value );
        }
        catch ( ClassCastException e )
        {
            throw new EvaluationException( String.format( "Expected expression of be of type `%s` but it was `%s`",
                    type.getCanonicalName(),
                    value.getClass().getCanonicalName() ), e );
        }
    }

    private Object map( AnyValue value ) throws EvaluationException
    {
        try
        {
            return value.map( MAPPER );
        }
        catch ( EvaluationRuntimeException e )
        {
            throw new EvaluationException( e.getMessage(), e );
        }
    }

    private static ValueMapper<Object> MAPPER = new ValueMapper.JavaMapper()
    {
        @Override
        public Object mapPath( PathValue value )
        {
            throw new EvaluationRuntimeException( "Unable to evaluate paths" );
        }

        @Override
        public Object mapNode( VirtualNodeValue value )
        {
            throw new EvaluationRuntimeException( "Unable to evaluate nodes" );
        }

        @Override
        public Object mapRelationship( VirtualRelationshipValue value )
        {
            throw new EvaluationRuntimeException( "Unable to evaluate relationships" );
        }
    };
}
