/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.extensions;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.PathRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;

abstract class ResultConverter
{
    static ResultConverter get( Type type )
    {
        return get( type, true );
    }

    private static ResultConverter get( Type type, boolean allowComplex )
    {
        if ( type instanceof Class<?> )
        {
            Class<?> cls = (Class<?>) type;
            if ( allowComplex && Representation.class.isAssignableFrom( cls ) )
            {
                return IDENTITY_RESULT;
            }
            else if ( cls == Node.class )
            {
                return NODE_RESULT;
            }
            else if ( cls == Relationship.class )
            {
                return RELATIONSHIP_RESULT;
            }
            else if ( cls == Path.class )
            {
                return PATH_RESULT;
            }
        }
        else if ( allowComplex && type instanceof ParameterizedType )
        {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Class<?> raw = (Class<?>) parameterizedType.getRawType();
            Type paramType = parameterizedType.getActualTypeArguments()[0];
            if ( !( paramType instanceof Class<?> ) )
            {
                throw new IllegalStateException(
                        "Parameterized result types must have a concrete type parameter." );
            }
            Class<?> param = (Class<?>) paramType;
            if ( Iterable.class.isAssignableFrom( raw ) )
            {
                return new ListResult( get( param, false ) );
            }
        }
        throw new IllegalStateException( "Illegal result type: " + type );
    }

    abstract Representation convert( Object obj );

    abstract RepresentationType type();

    private static final ResultConverter//
            IDENTITY_RESULT = new ResultConverter()
            {
                @Override
                Representation convert( Object obj )
                {
                    return (Representation) obj;
                }

                @Override
                RepresentationType type()
                {
                    return null;
                }
            },
            NODE_RESULT = new ResultConverter()
            {
                @Override
                Representation convert( Object obj )
                {
                    return new NodeRepresentation( (Node) obj );
                }

                @Override
                RepresentationType type()
                {
                    return RepresentationType.NODE;
                }
            }, RELATIONSHIP_RESULT = new ResultConverter()
            {
                @Override
                Representation convert( Object obj )
                {
                    return new RelationshipRepresentation( (Relationship) obj );
                }

                @Override
                RepresentationType type()
                {
                    return RepresentationType.RELATIONSHIP;
                }
            }, PATH_RESULT = new ResultConverter()
            {
                @Override
                Representation convert( Object obj )
                {
                    return new PathRepresentation( (Path) obj );
                }

                @Override
                RepresentationType type()
                {
                    return RepresentationType.PATH;
                }
            };

    private static class ListResult extends ResultConverter
    {
        private final ResultConverter itemConverter;

        ListResult( ResultConverter itemConverter )
        {
            this.itemConverter = itemConverter;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        Representation convert( Object obj )
        {
            return new ListRepresentation( itemConverter.type(),
                    new IterableWrapper<Representation, Object>( (Iterable<Object>) obj )
                    {
                        @Override
                        protected Representation underlyingObjectToObject( Object object )
                        {
                            return itemConverter.convert( object );
                        }
                    } );
        }

        @Override
        RepresentationType type()
        {
            return null;
        }
    }
}
