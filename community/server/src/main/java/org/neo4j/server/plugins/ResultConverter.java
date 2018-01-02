/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.plugins;

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
import org.neo4j.server.rest.repr.ValueRepresentation;

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
            else if ( cls == String.class )
            {
                return STRING_RESULT;
            }
            else if ( cls == void.class || cls == Void.class )
            {
                return VOID_RESULT;
            }
            else if ( cls == long.class || cls == Long.class )
            {
                return LONG_RESULT;
            }
            else if ( cls == double.class || cls == float.class || //
                      cls == Double.class || cls == Float.class )
            {
                return DOUBLE_RESULT;
            }
            else if ( cls == boolean.class || cls == Boolean.class )
            {
                return BOOL_RESULT;
            }
            else if ( cls == char.class || cls == Character.class )
            {
                return CHAR_RESULT;
            }
            else if ( cls.isPrimitive() || ( Number.class.isAssignableFrom( cls ) && //
                      cls.getPackage()
                              .getName()
                              .equals( "java.lang" ) ) )
            {
                return INT_RESULT;
            }
        }
        else if ( allowComplex && type instanceof ParameterizedType )
        {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Class<?> raw = (Class<?>) parameterizedType.getRawType();
            Type paramType = parameterizedType.getActualTypeArguments()[0];
            if ( !( paramType instanceof Class<?> ) )
            {
                throw new IllegalStateException( "Parameterized result types must have a concrete type parameter." );
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

    private static abstract class ValueResult extends ResultConverter
    {
        private final RepresentationType type;

        ValueResult( RepresentationType type )
        {
            this.type = type;
        }

        @Override
        final RepresentationType type()
        {
            return type;
        }
    }

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
            NODE_RESULT = new ValueResult( RepresentationType.NODE )
            {
                @Override
                Representation convert( Object obj )
                {
                    return new NodeRepresentation( (Node) obj );
                }
            }, RELATIONSHIP_RESULT = new ValueResult( RepresentationType.RELATIONSHIP )
            {
                @Override
                Representation convert( Object obj )
                {
                    return new RelationshipRepresentation( (Relationship) obj );
                }
            }, PATH_RESULT = new ValueResult( RepresentationType.PATH )
            {
                @Override
                Representation convert( Object obj )
                {
                    return new PathRepresentation<Path>( (Path) obj );
                }
            }, STRING_RESULT = new ValueResult( RepresentationType.STRING )
            {
                @Override
                Representation convert( Object obj )
                {
                    return ValueRepresentation.string( (String) obj );
                }
            }, LONG_RESULT = new ValueResult( RepresentationType.LONG )
            {
                @Override
                Representation convert( Object obj )
                {
                    return ValueRepresentation.number( ( (Number) obj ).longValue() );
                }
            }, DOUBLE_RESULT = new ValueResult( RepresentationType.DOUBLE )
            {
                @Override
                Representation convert( Object obj )
                {
                    return ValueRepresentation.number( ( (Number) obj ).doubleValue() );
                }
            }, BOOL_RESULT = new ValueResult( RepresentationType.BOOLEAN )
            {
                @Override
                @SuppressWarnings( "boxing" )
                Representation convert( Object obj )
                {
                    return ValueRepresentation.bool( (Boolean) obj );
                }
            }, INT_RESULT = new ValueResult( RepresentationType.INTEGER )
            {
                @Override
                Representation convert( Object obj )
                {
                    return ValueRepresentation.number( ( (Number) obj ).intValue() );
                }
            }, CHAR_RESULT = new ValueResult( RepresentationType.CHAR )
            {
                @Override
                @SuppressWarnings( "boxing" )
                Representation convert( Object obj )
                {
                    return ValueRepresentation.number( (Character) obj );
                }
            }, VOID_RESULT = new ValueResult( RepresentationType.NOTHING )
            {
                @Override
                Representation convert( Object obj )
                {
                    return Representation.emptyRepresentation();
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
            return new ListRepresentation( itemConverter.type(), new IterableWrapper<Representation, Object>(
                    (Iterable<Object>) obj )
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
