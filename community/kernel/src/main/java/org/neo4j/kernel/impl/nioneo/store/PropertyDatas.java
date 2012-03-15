/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.Arrays;
import org.neo4j.kernel.impl.cache.SizeOfs;

public class PropertyDatas
{
    private static abstract class PrimitivePropertyData implements PropertyData
    {
        private final int index;
        private final long id;

        PrimitivePropertyData( int index, long id )
        {
            this.index = index;
            this.id = id;
        }
        
        public int size()
        {
            // all primitives fit in 8 byte value
            // Object + id(long) + index(int) + value(pad)
            return 16 + 8 + 8;
        }
        
        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public int getIndex()
        {
            return index;
        }

        @Override
        public void setNewValue( Object newValue )
        {
            throw new IllegalStateException( "This shouldn't be called, " +
            		"only valid on String/array types" );
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + id + ",idx=" + index + ",value=" + getValue() + "]";
        }
    }

    private static class BooleanPropertyData extends PrimitivePropertyData
    {
        private final boolean value;

        private BooleanPropertyData( int index, long id, boolean value )
        {
            super( index, id );
            this.value = value;
        }

        @SuppressWarnings( "boxing" )
        @Override
        public Object getValue()
        {
            return value;
        }
    }

//    private static class LowBooleanPropertyData extends PrimitivePropertyData
//    {
//        // [viii,iiii][iiii,iiii]...and 6 more bytes of id...
//        private final long idAndValue;
//
//        private BooleanPropertyData( int index, long id, boolean value )
//        {
//            super( index );
//            this.idAndValue = id | ((long)((value?1:0))<<63);
//        }
//
//        @Override
//        public long getId()
//        {
//            return idAndValue&0x7FFFFFFFFFFFFFFFL;
//        }
//
//        @Override
//        public Object getValue()
//        {
//            return (idAndValue&0x8000000000000000L) != 0;
//        }
//    }

    private static class BytePropertyData extends PrimitivePropertyData
    {
        private final byte value;

        private BytePropertyData( int index, long id, byte value )
        {
            super( index, id );
            this.value = value;
        }

        @SuppressWarnings( "boxing" )
        @Override
        public Object getValue()
        {
            return value;
        }
    }

//    private static class LowBytePropertyData extends PrimitivePropertyData
//    {
//        // [vvvv,vvvv][iiii,iiii]...and 6 more bytes of id...
//        private final long idAndValue;
//
//        private BytePropertyData( int index, long id, byte value )
//        {
//            super( index );
//            this.idAndValue = id | (((long)value)<<56);
//        }
//
//        @Override
//        public long getId()
//        {
//            return idAndValue&0x00FFFFFFFFFFFFFFL;
//        }
//
//        @Override
//        public Object getValue()
//        {
//            return (byte)((idAndValue&0xFF00000000000000L)>>56);
//        }
//    }

    private static class ShortPropertyData extends PrimitivePropertyData
    {
        private final short value;

        private ShortPropertyData( int index, long id, short value )
        {
            super( index, id );
            this.value = value;
        }

        @SuppressWarnings( "boxing" )
        @Override
        public Object getValue()
        {
            return value;
        }
    }

//    private static class LowShortPropertyData extends PrimitivePropertyData
//    {
//        private final long idAndValue;
//
//        // [vvvv,vvvv][vvvv,vvvv][iiii,iiii]...and 5 more bytes of id...
//        private ShortPropertyData( int index, long id, short value )
//        {
//            super( index );
//            this.idAndValue = id | (((long)value)<<48);
//        }
//
//        @Override
//        public long getId()
//        {
//            return idAndValue&0x0000FFFFFFFFFFFFL;
//        }
//
//        @Override
//        public Object getValue()
//        {
//            return (short)((idAndValue&0xFFFF000000000000L)>>48);
//        }
//    }

    private static class CharPropertyData extends PrimitivePropertyData
    {
        private final char value;

        private CharPropertyData( int index, long id, char value )
        {
            super( index, id );
            this.value = value;
        }

        @SuppressWarnings( "boxing" )
        @Override
        public Object getValue()
        {
            return value;
        }
    }

//    private static class LowCharPropertyData extends PrimitivePropertyData
//    {
//        private final long idAndValue;
//
//        // [vvvv,vvvv][vvvv,vvvv][iiii,iiii]...and 5 more bytes of id...
//        private CharPropertyData( int index, long id, char value )
//        {
//            super( index );
//            this.idAndValue = id | (((long)value)<<48);
//        }
//
//        @Override
//        public long getId()
//        {
//            return idAndValue&0x0000FFFFFFFFFFFFL;
//        }
//
//        @Override
//        public Object getValue()
//        {
//            return (char)((idAndValue&0xFFFF000000000000L)>>48);
//        }
//    }

//    private static class LowIntPropertyData extends PrimitivePropertyData
//    {
//        // A value fitting in here must be less than this
//        private static final int LIMIT = (int) Math.pow( 2, 28 );
//
//        // Property ID is max 36 bits so there are 28 bits left for an int.
//        // Wether it fits or not is decided by the called of this constructor.
//        // [vvvv,vvvv][vvvv,vvvv][vvvv,vvvv][vvvv,iiii][iiii,iiii]...3 more bytes id...
//        private final long idAndValue;
//
//        private LowIntPropertyData( int index, long id, int value )
//        {
//            super( index );
//            this.idAndValue = id | (((long)value)<<36);
//        }
//
//        @Override
//        public long getId()
//        {
//            return idAndValue&0xFFFFFFFFFL;
//        }
//
//        @Override
//        public Object getValue()
//        {
//            return (int)((idAndValue&0xFFFFFFF000000000L)>>36);
//        }
//    }

    private static class IntPropertyData extends PrimitivePropertyData
    {
        private final int value;

        private IntPropertyData( int index, long id, int value )
        {
            super( index, id );
            this.value = value;
        }

        @SuppressWarnings( "boxing" )
        @Override
        public Object getValue()
        {
            return value;
        }
    }

//    private static class LowLongPropertyData extends PrimitivePropertyData
//    {
//        // A value fitting in here must be less than this
//        private static final long LIMIT = (long) Math.pow( 2, 28 );
//
//        private final long idAndValue;
//
//        private LowLongPropertyData( int index, long id, long value )
//        {
//            super( index );
//            this.idAndValue = id | (value<<36);
//        }
//
//        @Override
//        public long getId()
//        {
//            return idAndValue&0xFFFFFFFFFL;
//        }
//
//        @Override
//        public Object getValue()
//        {
//            return (idAndValue&0xFFFFFFF000000000L)>>36;
//        }
//    }

    private static class LongPropertyData extends PrimitivePropertyData
    {
        private final long value;

        private LongPropertyData( int index, long id, long value )
        {
            super( index, id );
            this.value = value;
        }
        
        public int size()
        {
            return super.size() + 8;
        }

        @SuppressWarnings( "boxing" )
        @Override
        public Object getValue()
        {
            return value;
        }
    }

    private static class FloatPropertyData extends PrimitivePropertyData
    {
        private final float value;

        private FloatPropertyData( int index, long id, float value )
        {
            super( index, id );
            this.value = value;
        }

        @Override
        public Object getValue()
        {
            return value;
        }
    }

    private static class DoublePropertyData extends PrimitivePropertyData
    {
        private final double value;

        private DoublePropertyData( int index, long id, double value )
        {
            super( index, id );
            this.value = value;
        }

        public int size()
        {
            return super.size() + 8;
        }
        
        @SuppressWarnings( "boxing" )
        @Override
        public Object getValue()
        {
            return value;
        }
    }
    
    private static int sizeOf( Object value )
    {
        if ( value == null )
        {
            return 0;
        }
        if ( value instanceof String )
        {
            return SizeOfs.sizeOf( (String) value );
        }
        else if ( value.getClass().isArray() )
        {
            return SizeOfs.sizeOfArray( value );
        }
        else
        {
            throw new IllegalStateException( "Unkown type: " + value.getClass() + " [" + value + "]" ); 
        }
    }
    
    private static class ObjectPropertyData implements PropertyData
    {
        private final long id;
        private Object value;
        private final int index;

        public ObjectPropertyData( int index, long id, Object value )
        {
            this.index = index;
            this.id = id;
            this.value = value;
        }
        
        public int size()
        {
            // Object + id(long) + value(Object) + index(int)
            return 16 + 8 + 8 + 4 + sizeOf( value );
        }

        @Override
        public long getId()
        {
            return id;
        }

        public int getIndex()
        {
            return index;
        }

        @Override
        public Object getValue()
        {
            return value;
        }

        @Override
        public void setNewValue( Object newValue )
        {
            this.value = newValue;
        }

        @Override
        public String toString()
        {
            String val;
            if ( value == null )
            {
                val = "null";
            }
            else if ( value instanceof Object[] )
            {
                val = Arrays.toString( (Object[]) value );
            }
            else if ( value.getClass().isArray() )
            {
                val = Arrays.deepToString( new Object[] { value } );
                val = val.substring( 1, val.length() - 1 );
            }
            else
            {
                val = value.toString();
            }
            return "PropertyData[" + id + ",idx=" + index + ",value=" + val + "]";
        }
    }

    public static PropertyData forBoolean( int index, long id, boolean value )
    {
        return new BooleanPropertyData( index, id, value );
    }

    public static PropertyData forByte( int index, long id, byte value )
    {
        return new BytePropertyData( index, id, value );
    }

    public static PropertyData forShort( int index, long id, short value )
    {
        return new ShortPropertyData( index, id, value );
    }

    public static PropertyData forChar( int index, long id, char value )
    {
        return new CharPropertyData( index, id, value );
    }

    public static PropertyData forInt( int index, long id, int value )
    {
        return new IntPropertyData( index, id, value );
    }

    public static PropertyData forLong( int index, long id, long value )
    {
        return new LongPropertyData( index, id, value );
    }

    public static PropertyData forFloat( int index, long id, float value )
    {
        return new FloatPropertyData( index, id, value );
    }

    public static PropertyData forDouble( int index, long id, double value )
    {
        return new DoublePropertyData( index, id, value );
    }

    public static PropertyData forStringOrArray( int index, long id, Object value )
    {
        return new ObjectPropertyData( index, id, value );
    }
}
