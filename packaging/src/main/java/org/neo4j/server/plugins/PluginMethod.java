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

package org.neo4j.server.plugins;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.Representation;

class PluginMethod extends ExtensionPoint
{
    private final ServerPlugin plugin;
    private final Method method;
    private final DataExtractor[] extractors;
    private final ResultConverter result;

    private PluginMethod( String name, Class<?> discovery, ServerPlugin plugin,
                          ResultConverter result, Method method, DataExtractor[] extractors,
                          Description description )
    {
        super( discovery, name, description == null ? "" : description.value() );
        this.plugin = plugin;
        this.result = result;
        this.method = method;
        this.extractors = extractors;
    }

    static PluginMethod createFrom( ServerPlugin plugin, Method method,
            Class<?> discovery )
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
                throw new IllegalStateException(
                        String.format(
                                "Method parameter %d of %s cannot be retrieved as both Parameter and Source",
                                Integer.valueOf( i ), method ) );
            }
            else if ( source != null )
            {
                if ( types[i] != discovery )
                {
                    throw new IllegalStateException(
                            "Source parameter type must equal the discovery type." );
                }
                if ( sourceExtractor != null )
                {
                    throw new IllegalStateException(
                            "Server Extension methods may have at most one Source parameter." );
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
        return new PluginMethod( nameOf( method ), discovery, plugin, result, method,
                extractors, method.getAnnotation( Description.class ) );
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

    private static abstract class DataExtractor
    {
        abstract Object extract( AbstractGraphDatabase graphDb, Object source,
                ParameterList parameters ) throws BadInputException;

        void describe( ParameterDescriptionConsumer consumer )
        {
        }
    }

    private static class SourceExtractor extends DataExtractor
    {
        SourceExtractor( Source source, Description description )
        {
        }

        @Override
        Object extract( AbstractGraphDatabase graphDb, Object source, ParameterList parameters )
        {
            return source;
        }
    }

    private static ParameterExtractor parameterExtractor( Type type, Parameter parameter,
            Description description )
    {
        if ( description instanceof ParameterizedType )
        {
            ParameterizedType paramType = (ParameterizedType) description;
            Class<?> raw = (Class<?>) paramType.getRawType();
            if ( Set.class.isAssignableFrom( raw ) )
            {
                TypeCaster caster = TYPES.get( raw );
                if ( caster != null )
                {
                    return new ListParameterExtractor( caster, raw, parameter, description )
                    {
                        @Override
                        Object convert( Object[] result )
                        {
                            return new HashSet<Object>( Arrays.asList( result ) );
                        }
                    };
                }
            }
            else if ( Iterable.class.isAssignableFrom( raw ) )
            {
                TypeCaster caster = TYPES.get( raw );
                if ( caster != null )
                {
                    return new ListParameterExtractor( caster, raw, parameter, description )
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
                    return new ListParameterExtractor( caster, raw, parameter, description )
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
        throw new IllegalStateException( "Unsupported parameter type: " + type );
    }

    private static class ParameterExtractor extends DataExtractor
    {
        final String name;
        final Class<?> type;
        final boolean optional;
        final String description;
        final TypeCaster caster;

        ParameterExtractor( TypeCaster caster, Class<?> type, Parameter param,
                Description description )
        {
            this.caster = caster;
            this.type = type;
            this.name = param.name();
            this.optional = param.optional();
            this.description = description == null ? "" : description.value();
        }

        @Override
        Object extract( AbstractGraphDatabase graphDb, Object source, ParameterList parameters )
                throws BadInputException
        {
            Object result = caster.get( graphDb, parameters, name );
            if ( optional || result != null ) return result;
            throw new IllegalArgumentException( "Mandatory argument \"" + name + "\" not supplied." );
        }

        @Override
        void describe( ParameterDescriptionConsumer consumer )
        {
            consumer.describeParameter( name, type, optional, description );
        }
    }

    private static abstract class ListParameterExtractor extends ParameterExtractor
    {
        ListParameterExtractor( TypeCaster caster, Class<?> type, Parameter param,
                Description description )
        {
            super( caster, type, param, description );
        }

        @Override
        Object extract( AbstractGraphDatabase graphDb, Object source, ParameterList parameters )
                throws BadInputException
        {
            Object[] result = caster.getList( graphDb, parameters, name );
            if ( result != null ) return convert( result );
            if ( optional ) return null;
            throw new IllegalArgumentException( "Mandatory argument \"" + name + "\" not supplied." );
        }

        abstract Object convert( Object[] result );

        @Override
        void describe( ParameterDescriptionConsumer consumer )
        {
            consumer.describeListParameter( name, type, optional, description );
        }
    }

    private interface TypeCaster
    {
        Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                throws BadInputException;

        Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                throws BadInputException;
    }

    private static final Map<Class<?>, TypeCaster> TYPES = new HashMap<Class<?>, TypeCaster>();
    static
    {
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getString( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getStringList( name );
            }
        }, String.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getByte( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getByteList( name );
            }
        }, byte.class, Byte.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getShort( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getShortList( name );
            }
        }, short.class, Short.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getInteger( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getIntegerList( name );
            }
        }, int.class, Integer.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getLong( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getLongList( name );
            }
        }, long.class, Long.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getCharacter( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getCharacterList( name );
            }
        }, char.class, Character.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getBoolean( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getBooleanList( name );
            }
        }, boolean.class, Boolean.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getFloat( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getFloatList( name );
            }
        }, float.class, Float.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getDouble( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getDoubleList( name );
            }
        }, double.class, Double.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getNode( graphDb, name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getNodeList( graphDb, name );
            }
        }, Node.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getRelationship( graphDb, name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getRelationshipList( graphDb, name );
            }
        }, Relationship.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                return parameters.getUri( name );
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                return parameters.getUriList( name );
            }
        }, URI.class );
        put( TYPES, new TypeCaster()
        {
            @Override
            public Object get( AbstractGraphDatabase graphDb, ParameterList parameters, String name )
                    throws BadInputException
            {
                try
                {
                    return parameters.getUri( name ).toURL();
                }
                catch ( MalformedURLException e )
                {
                    throw new BadInputException( e );
                }
            }

            @Override
            public Object[] getList( AbstractGraphDatabase graphDb, ParameterList parameters,
                    String name ) throws BadInputException
            {
                URI[] uris = parameters.getUriList( name );
                URL[] urls = new URL[uris.length];
                try
                {
                    for ( int i = 0; i < urls.length; i++ )
                    {
                        urls[i] = uris[i].toURL();
                    }
                }
                catch ( MalformedURLException e )
                {
                    throw new BadInputException( e );
                }
                return urls;
            }
        }, URL.class );
    }

    private static void put( Map<Class<?>, TypeCaster> types, TypeCaster caster, Class<?>... keys )
    {
        for ( Class<?> key : keys )
        {
            types.put( key, caster );
        }
    }

    @Override
    public Representation invoke( AbstractGraphDatabase graphDb, Object source, ParameterList params )
            throws BadExtensionInvocationException, ExtensionInvocationFailureException,
            BadInputException
    {
        Object[] arguments = new Object[extractors.length];
        for ( int i = 0; i < arguments.length; i++ )
        {
            arguments[i] = extractors[i].extract( graphDb, source, params );
        }
        try
        {
            return result.convert( method.invoke( plugin, arguments ) );
        }
        catch ( InvocationTargetException exc )
        {
            Throwable targetExc = exc.getTargetException();
            for ( Class<?> excType : method.getExceptionTypes() )
            {
                if ( excType.isInstance( targetExc ) )
                    throw new BadExtensionInvocationException( targetExc );
            }
            throw new ExtensionInvocationFailureException( targetExc );
        }
        catch ( IllegalArgumentException e )
        {
            throw new ExtensionInvocationFailureException( e );
        }
        catch ( IllegalAccessException e )
        {
            throw new ExtensionInvocationFailureException( e );
        }
    }

    @Override
    protected void describeParameters( ParameterDescriptionConsumer consumer )
    {
        for ( DataExtractor extractor : extractors )
        {
            extractor.describe( consumer );
        }
    }
}
