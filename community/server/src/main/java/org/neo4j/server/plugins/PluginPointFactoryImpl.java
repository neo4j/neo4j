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

import java.lang.annotation.Annotation;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

class PluginPointFactoryImpl implements PluginPointFactory
{

    public PluginPoint createFrom( ServerPlugin plugin, Method method, Class<?> discovery )
    {
        ResultConverter result = ResultConverter.get( method.getGenericReturnType() );
        Type[] types = method.getGenericParameterTypes();
        Annotation[][] annotations = method.getParameterAnnotations();
        SourceExtractor sourceExtractor = null;
        DataExtractor[] extractors = new DataExtractor[types.length];
        for ( int i = 0; i < types.length; i++ )
        {
            Description description = null;
            Parameter param = null;
            Source source = null;
            for ( Annotation annotation : annotations[i] )
            {
                if ( annotation instanceof Description )
                {
                    description = (Description) annotation;
                }
                else if ( annotation instanceof Parameter )
                {
                    param = (Parameter) annotation;
                }
                else if ( annotation instanceof Source )
                {
                    source = (Source) annotation;
                }
            }
            if ( param != null && source != null )
            {
                throw new IllegalStateException( String.format(
                        "Method parameter %d of %s cannot be retrieved as both Parameter and Source",
                        Integer.valueOf( i ), method ) );
            }
            else if ( source != null )
            {
                if ( types[i] != discovery )
                {
                    throw new IllegalStateException( "Source parameter type (" + types[i]
                                                     + ") must equal the discovery type (" + discovery.getName() + ")." );
                }
                if ( sourceExtractor != null )
                {
                    throw new IllegalStateException( "Server Extension methods may have at most one Source parameter." );
                }
                extractors[i] = sourceExtractor = new SourceExtractor( source, description );
            }
            else if ( param != null )
            {
                extractors[i] = parameterExtractor( types[i], param, description );
            }
            else
            {
                throw new IllegalStateException(
                        "Parameters of Server Extension methods must be annotated as either Source or Parameter." );
            }
        }
        return new PluginMethod( nameOf( method ), discovery, plugin, result, method, extractors,
                method.getAnnotation( Description.class ) );
    }

    private static ParameterExtractor parameterExtractor( Type type, Parameter parameter, Description description )
    {
        if ( type instanceof ParameterizedType )
        {
            ParameterizedType paramType = (ParameterizedType) type;
            Class<?> raw = (Class<?>) paramType.getRawType();
            Type componentType = paramType.getActualTypeArguments()[0];
            Class<?> component = null;
            if ( componentType instanceof Class<?> )
            {
                component = (Class<?>) componentType;
            }
            if ( Set.class == raw )
            {
                TypeCaster caster = TYPES.get( component );
                if ( caster != null )
                {
                    return new ListParameterExtractor( caster, component, parameter, description )
                    {
                        @Override
                        Object convert( Object[] result )
                        {
                            return new HashSet<Object>( Arrays.asList( result ) );
                        }
                    };
                }
            }
            else if ( List.class == raw || Collection.class == raw || Iterable.class == raw )
            {
                TypeCaster caster = TYPES.get( component );
                if ( caster != null )
                {
                    return new ListParameterExtractor( caster, component, parameter, description )
                    {
                        @Override
                        Object convert( Object[] result )
                        {
                            return Arrays.asList( result );
                        }
                    };
                }
            }
        }
        else if ( type instanceof Class<?> )
        {
            Class<?> raw = (Class<?>) type;
            if ( raw.isArray() )
            {
                TypeCaster caster = TYPES.get( raw.getComponentType() );
                if ( caster != null )
                {
                    return new ListParameterExtractor( caster, raw.getComponentType(), parameter, description )
                    {
                        @Override
                        Object convert( Object[] result )
                        {
                            return result;
                        }
                    };
                }
            }
            else
            {
                TypeCaster caster = TYPES.get( raw );
                if ( caster != null )
                {
                    return new ParameterExtractor( caster, raw, parameter, description );
                }
            }
        }
        else if ( type instanceof GenericArrayType )
        {
            GenericArrayType array = (GenericArrayType) type;
            Type component = array.getGenericComponentType();
            if ( component instanceof Class<?> )
            {
                TypeCaster caster = TYPES.get( component );
                if ( caster != null )
                {
                    return new ListParameterExtractor( caster, (Class<?>) component, parameter, description )
                    {
                        @Override
                        Object convert( Object[] result )
                        {
                            return result;
                        }
                    };
                }
            }
        }
        throw new IllegalStateException( "Unsupported parameter type: " + type );
    }

    private static void put( Map<Class<?>, TypeCaster> types, TypeCaster caster, Class<?>... keys )
    {
        for ( Class<?> key : keys )
        {
            types.put( key, caster );
        }
    }

    private static final Map<Class<?>, TypeCaster> TYPES = new HashMap<Class<?>, TypeCaster>();
    static
    {
        put( TYPES, new StringTypeCaster(), String.class );
        put( TYPES, new ByteTypeCaster(), byte.class, Byte.class );
        put( TYPES, new ShortTypeCaster(), short.class, Short.class );
        put( TYPES, new IntegerTypeCaster(), int.class, Integer.class );
        put( TYPES, new LongTypeCaster(), long.class, Long.class );
        put( TYPES, new CharacterTypeCaster(), char.class, Character.class );
        put( TYPES, new BooleanTypeCaster(), boolean.class, Boolean.class );
        put( TYPES, new FloatTypeCaster(), float.class, Float.class );
        put( TYPES, new DoubleTypeCaster(), double.class, Double.class );
        put( TYPES, new MapTypeCaster(), Map.class );
        put( TYPES, new NodeTypeCaster(), Node.class );
        put( TYPES, new RelationshipTypeCaster(), Relationship.class );
        put( TYPES, new RelationshipTypeTypeCaster(), RelationshipType.class );
        put( TYPES, new UriTypeCaster(), URI.class );
        put( TYPES, new URLTypeCaster(), URL.class );
    }

    private static String nameOf( Method method )
    {
        Name name = method.getAnnotation( Name.class );
        if ( name != null )
        {
            return name.value();
        }
        return method.getName();
    }

}
