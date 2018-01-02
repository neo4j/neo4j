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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;

/**
 * Utility for reading and writing property values from/into a channel. Supports neo4j property types,
 * including arrays.
 */
public abstract class ValueType
{
    private static final Map<Class<?>,ValueType> byClass = new HashMap<>();
    private static final Map<Byte,ValueType> byId = new HashMap<>();
    private static ValueType stringType;
    private static byte next = 0;
    static
    {
        add( new ValueType( Boolean.TYPE, Boolean.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return from.get() == 0 ? Boolean.FALSE : Boolean.TRUE;
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.put( (Boolean)value ? (byte)1 : (byte)0 );
            }
        } );
        add( new ValueType( Byte.TYPE, Byte.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return from.get();
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.put( (Byte)value );
            }
        } );
        add( new ValueType( Short.TYPE, Short.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return from.getShort();
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.putShort( (Short)value );
            }
        } );
        add( new ValueType( Character.TYPE, Character.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return (char)from.getInt();
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.putInt( (Character)value );
            }
        } );
        add( new ValueType( Integer.TYPE, Integer.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return from.getInt();
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.putInt( (Integer) value );
            }
        } );
        add( new ValueType( Long.TYPE, Long.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return from.getLong();
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.putLong( (Long)value );
            }
        } );
        add( new ValueType( Float.TYPE, Float.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return from.getFloat();
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.putFloat( (Float)value );
            }
        } );
        add( stringType = new ValueType( String.class )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                int length = from.getInt();
                byte[] bytes = new byte[length]; // TODO wasteful
                from.get( bytes, length );
                return UTF8.decode( bytes );
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                byte[] bytes = UTF8.encode( (String)value );
                into.putInt( bytes.length ).put( bytes, bytes.length );
            }
        } );
        add( new ValueType( Double.class, Double.TYPE )
        {
            @Override
            public Object read( ReadableLogChannel from ) throws IOException
            {
                return from.getDouble();
            }

            @Override
            public void write( Object value, WritableLogChannel into ) throws IOException
            {
                into.putDouble( (Double)value );
            }
        } );
    }
    private static final ValueType arrayType = new ValueType()
    {
        @Override
        public Object read( ReadableLogChannel from ) throws IOException
        {
            ValueType componentType = typeOf( from.get() );
            int length = from.getInt();
            Object value = Array.newInstance( componentType.componentClass(), length );
            for ( int i = 0; i < length; i++ )
            {
                Array.set( value, i, componentType.read( from ) );
            }
            return value;
        }

        @Override
        public void write( Object value, WritableLogChannel into ) throws IOException
        {
            ValueType componentType = typeOf( value.getClass().getComponentType() );
            into.put( componentType.id() );
            int length = Array.getLength( value );
            into.putInt( length );
            for ( int i = 0; i < length; i++ )
            {
                componentType.write( Array.get( value, i ), into );
            }
        }
    };

    private final Class<?>[] classes;
    private final byte id = next++;

    private ValueType( Class<?>... classes )
    {
        this.classes = classes;
    }

    private static void add( ValueType type )
    {
        for ( Class<?> cls : type.classes )
        {
            byClass.put( cls, type );
        }
        byId.put( type.id(), type );
    }

    public static ValueType typeOf( Object value )
    {
        return typeOf( value.getClass() );
    }

    public static ValueType typeOf( Class<?> cls )
    {
        if ( cls.isArray() )
        {
            return arrayType;
        }

        ValueType type = byClass.get( cls );
        assert type != null : "Unrecognized value type " + cls;
        return type;
    }

    public static ValueType typeOf( byte id )
    {
        if ( id == arrayType.id() )
        {
            return arrayType;
        }

        ValueType type = byId.get( id );
        assert type != null : "Unrecognized value type id " + id;
        return type;
    }

    public static ValueType stringType()
    {
        return stringType;
    }

    public Class<?> componentClass()
    {
        return classes[0];
    }

    public final byte id()
    {
        return id;
    }

    public abstract Object read( ReadableLogChannel from ) throws IOException;

    public abstract void write( Object value, WritableLogChannel into ) throws IOException;
}
