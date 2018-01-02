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
package org.neo4j.server.rest.repr;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ObjectRepresentation extends MappingRepresentation
{
    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    protected @interface Mapping
    {
        String value();
    }

    private final static ConcurrentHashMap<Class<?>, Map<String, PropertyGetter>> serializations = new ConcurrentHashMap<Class<?>, Map<String, PropertyGetter>>();

    private final Map<String, PropertyGetter> serialization = serialization( getClass() );

    ObjectRepresentation( RepresentationType type )
    {
        super( type );
    }

    public ObjectRepresentation( String type )
    {
        super( type );
    }

    private static Map<String, PropertyGetter> serialization( Class<? extends ObjectRepresentation> type )
    {
        Map<String, PropertyGetter> serialization = serializations.get( type );
        if ( serialization == null )
        {
            synchronized ( type )
            {
                serialization = serializations.get( type );
                if ( serialization == null )
                {
                    serialization = new HashMap<String, PropertyGetter>();
                    for ( Method method : type.getMethods() )
                    {
                        Mapping property = method.getAnnotation( Mapping.class );
                        if ( property != null )
                        {
                            serialization.put( property.value(), getter( method ) );
                        }
                    }
                    serializations.put(type, serialization);
                }
            }
        }
        return serialization;
    }

    private static PropertyGetter getter( final Method method )
    {
        // If this turns out to be a bottle neck we could use a byte code
        // generation library, such as ASM, instead of reflection.
        return new PropertyGetter( method )
        {
            @Override
            Object get( ObjectRepresentation object )
            {
                Throwable e;
                try
                {
                    return method.invoke( object );
                }
                catch ( InvocationTargetException ex )
                {
                    e = ex.getTargetException();
                    if ( e instanceof RuntimeException )
                    {
                        throw (RuntimeException) e;
                    }
                    else if ( e instanceof Error )
                    {
                        throw (Error) e;
                    }
                }
                catch ( Exception ex )
                {
                    e = ex;
                }
                throw new IllegalStateException( "Serialization failure", e );
            }
        };
    }

    private static abstract class PropertyGetter
    {
        PropertyGetter( Method method )
        {
            if ( method.getParameterTypes().length != 0 )
            {
                throw new IllegalStateException( "Property getter method may not have any parameters." );
            }
            if ( !Representation.class.isAssignableFrom( (Class<?>) method.getReturnType() ) )
            {
                throw new IllegalStateException( "Property getter must return Representation object." );
            }
        }

        void putTo( MappingSerializer serializer, ObjectRepresentation object, String key )
        {
            Object value = get( object );
            if ( value != null ) ( (Representation) value ).putTo( serializer, key );
        }

        abstract Object get( ObjectRepresentation object );
    }

    @Override
    protected final void serialize( MappingSerializer serializer )
    {
        for ( Map.Entry<String, PropertyGetter> property : serialization.entrySet() )
        {
            property.getValue()
                    .putTo( serializer, this, property.getKey() );
        }
        extraData( serializer );
    }

    void extraData( MappingSerializer serializer )
    {
    }
}
