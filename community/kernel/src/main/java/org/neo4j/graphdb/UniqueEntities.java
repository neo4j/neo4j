/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphdb;

import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;

import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;

public class UniqueEntities
{
    public static class Result<T>
    {
        private final T entity;
        private final boolean wasCreated;

        public Result( T entity, boolean wasCreated )
        {
            this.entity = entity;
            this.wasCreated = wasCreated;
        }

        public T entity()
        {
            return entity;
        }

        public boolean wasCreated()
        {
            return wasCreated;
        }
    }

    public static interface Creator<T>
    {
        Result<T> getOrCreate( Object value );
    }

    public static Creator<Node> byUniquenessConstraint( final GraphDatabaseService db,
                                                        final ConstraintDefinition constraint )
    {
        final Label label = constraint.getLabel();
        final String propertyKey = single( constraint.getPropertyKeys() );

        if ( ! constraint.isConstraintType( ConstraintType.UNIQUENESS ) )
        {
            throw new IllegalArgumentException( "Expected ConstraintDefinition of type ConstraintType.UNIQUENESS" );
        }

        return new Creator<Node>()
        {
            @Override
            public Result<Node> getOrCreate( Object value )
            {
                Node node = singleOrNull( db.findNodesByLabelAndProperty( label, propertyKey, value ) );
                if ( null == node )
                {
                    node = db.createNode( label );
                    node.setProperty( propertyKey, value );
                    return new Result<>( node, true);
                }
                else
                {
                    return new Result<>( node, false );
                }
            }
        };
    }

    public static <T extends PropertyContainer> Creator<T> byLegacyUniquenessFactory( final UniqueFactory<T> factory,
                                                                                      final String propertyKey )
    {
        return new Creator<T>()
        {
            @Override
            public Result<T> getOrCreate( Object value )
            {
                return factory.getOrCreateWithOutcome( propertyKey, value );
            }
        };
    }
}
