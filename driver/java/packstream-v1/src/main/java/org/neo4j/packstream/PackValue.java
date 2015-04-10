/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.packstream;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class PackValue implements Iterable<PackValue>
{
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final List<PackValue> EMPTY_LIST_OF_VALUES = Collections.EMPTY_LIST;
    public static final Map<String, PackValue> EMPTY_MAP_OF_VALUES = Collections.EMPTY_MAP;

    public boolean isNull() { return false; }

    public boolean isBoolean() { return false; }

    public boolean isInteger() { return false; }

    public boolean isFloat() { return false; }

    public boolean isBytes() { return false; }

    public boolean isText() { return false; }

    public boolean isList() { return false; }

    public boolean isMap() { return false; }

    public boolean isStruct() { return false; }

    public abstract boolean booleanValue();

    public abstract int intValue();

    public abstract long longValue();

    public abstract float floatValue();

    public abstract double doubleValue();

    public abstract byte[] byteArrayValue();

    public abstract String stringValue();

    public int size() {
        return 1;
    }

    public char signature() {
        return '\0';
    }

    public PackValue get( int index )
    {
        throw new UnsupportedOperationException( "Value is not indexable" );
    }

    public PackValue get( String key )
    {
        throw new UnsupportedOperationException( "Value is not keyed" );
    }

    public List<PackValue> listValue()
    {
        throw new UnsupportedOperationException( "Value is not iterable" );
    }

    public Map<String,PackValue> mapValue()
    {
        throw new UnsupportedOperationException( "Value is not iterable" );
    }

    @Override
    public Iterator<PackValue> iterator()
    {
        throw new UnsupportedOperationException( "Value is not iterable" );
    }

    @Override
    public String toString()
    {
        return this.stringValue();
    }

    public static class NullValue extends PackValue
    {
        public static final NullValue NULL_VALUE = new NullValue();

        @Override
        public boolean isNull() { return true; }

        @Override
        public boolean booleanValue() { return false; }

        @Override
        public int intValue() { return 0; }

        @Override
        public long longValue() { return 0; }

        @Override
        public float floatValue() { return 0; }

        @Override
        public double doubleValue() { return 0; }

        @Override
        public byte[] byteArrayValue() { return new byte[0]; }

        @Override
        public String stringValue() { return null; }

        @Override
        public int size()
        {
            return 0;
        }

    }

    public static class BooleanValue extends PackValue
    {

        public static final BooleanValue TRUE = new BooleanValue( true );
        public static final BooleanValue FALSE = new BooleanValue( false );

        private final boolean value;

        private BooleanValue( boolean value ) { this.value = value; }

        @Override
        public boolean isBoolean() { return true; }

        @Override
        public boolean booleanValue() { return value; }

        @Override
        public int intValue() { return value ? 1 : 0; }

        @Override
        public long longValue() { return value ? 1 : 0; }

        @Override
        public float floatValue() { return value ? 1 : 0; }

        @Override
        public double doubleValue() { return value ? 1 : 0; }

        @Override
        public byte[] byteArrayValue() { return null; }

        @Override
        public String stringValue() { return value ? "true" : "false"; }

    }

    public static class IntegerValue extends PackValue
    {
        // Integer cache
        private static final IntegerValue[] positiveValues = new IntegerValue[256];
        private static final IntegerValue[] negativeValues = new IntegerValue[256];
        static
        {
            // Keep hold of small values so instances can be reused
            for ( int i = 0; i < positiveValues.length; i++ )
            {
                positiveValues[i] = new IntegerValue( i );
            }
            for ( int i = 0; i < negativeValues.length; i++ )
            {
                negativeValues[i] = new IntegerValue( -i );
            }
        }

        public static IntegerValue getInstance( long value )
        {
            if ( value >= positiveValues.length )
            {
                return new IntegerValue( value );
            }
            else if ( value >= 0 )
            {
                return positiveValues[(int) value];
            }
            else
            {
                long absoluteValue = -value;
                if ( absoluteValue >= negativeValues.length )
                {
                    return new IntegerValue( value );
                }
                else
                {
                    return negativeValues[(int) absoluteValue];
                }
            }
        }

        private final long value;

        private IntegerValue( long value ) { this.value = value; }

        @Override
        public boolean isInteger() { return true; }

        @Override
        public boolean booleanValue() { return value != 0; }

        @Override
        public int intValue() { return (int) value; }

        @Override
        public long longValue() { return value; }

        @Override
        public float floatValue() { return (float) value; }

        @Override
        public double doubleValue() { return (double) value; }

        @Override
        public byte[] byteArrayValue() { return new byte[0]; }

        @Override
        public String stringValue() { return Long.toString( value ); }

    }

    public static class FloatValue extends PackValue
    {

        private final double value;

        public FloatValue( double value ) { this.value = value; }

        @Override
        public boolean isFloat() { return true; }

        @Override
        public boolean booleanValue() { return value != 0; }

        @Override
        public int intValue() { return (int) value; }

        @Override
        public long longValue() { return (long) value; }

        @Override
        public float floatValue() { return (float) value; }

        @Override
        public double doubleValue() { return value; }

        @Override
        public byte[] byteArrayValue() { return new byte[0]; }

        @Override
        public String stringValue() { return Double.toString( value ); }

    }

    public static class BytesValue extends PackValue
    {
        private static final BytesValue EMPTY_BYTES = new BytesValue( EMPTY_BYTE_ARRAY );

        public static BytesValue getInstance( byte[] value )
        {
            if ( value.length == 0 )
            {
                return EMPTY_BYTES;
            }
            else
            {
                return new BytesValue( value );
            }
        }

        private final byte[] value;

        private BytesValue( byte[] value ) { this.value = value; }

        @Override
        public boolean isBytes() { return true; }

        @Override
        public boolean booleanValue() { return value.length > 0; }

        @Override
        public int intValue() { throw cantConvertTo( "int" ); }

        @Override
        public long longValue() { throw cantConvertTo( "long" ); }

        @Override
        public float floatValue() { throw cantConvertTo( "float" ); }

        @Override
        public double doubleValue() { throw cantConvertTo( "double" ); }

        @Override
        public byte[] byteArrayValue() { return value; }

        @Override
        public String stringValue() { return new String( value ); }

        @Override
        public PackValue get( int index )
        {
            if ( index >= 0 && index < value.length )
            {
                return new IntegerValue( value[index] );
            }
            else
            {
                throw new ArrayIndexOutOfBoundsException(
                        "Index must be between 0 and " + value.length );
            }
        }

        @Override
        public int size()
        {
            return value.length;
        }

        private UnsupportedOperationException cantConvertTo( String toTarget )
        {
            return new UnsupportedOperationException( "Bytes value cannot be converted to "+toTarget+"." );
        }

    }

    public static class TextValue extends PackValue
    {

        private static final TextValue EMPTY_TEXT = new TextValue( EMPTY_BYTE_ARRAY );

        public static TextValue getInstance( char value )
        {
            return new TextValue( value );
        }

        public static TextValue getInstance( byte[] value )
        {
            return new TextValue( value );
        }

        private final byte[] utf8;
        private String decoded;

        private TextValue( char value ) { this( Character.toString( value ).getBytes( UTF_8 ) ); }

        private TextValue( byte[] utf8 ) { this.utf8 = utf8; }

        @Override
        public boolean isText() { return true; }

        @Override
        public boolean booleanValue() { return stringValue().length() > 0; }

        @Override
        public int intValue() { return Integer.parseInt( stringValue() ); }

        @Override
        public long longValue() { return Long.parseLong( stringValue() ); }

        @Override
        public float floatValue() { return Float.parseFloat( stringValue() ); }

        @Override
        public double doubleValue() { return Double.parseDouble( stringValue() ); }

        @Override
        public byte[] byteArrayValue() { return utf8; }

        @Override
        public String stringValue()
        {
            if(decoded == null)
            {
                decoded = new String( utf8, UTF_8 );
            }
            return decoded;
        }

        @Override
        public PackValue get( int index )
        {
            String value = stringValue();
            if ( index >= 0 && index < value.length() )
            {
                return new TextValue( value.charAt( index ) );
            }
            else
            {
                throw new ArrayIndexOutOfBoundsException(
                        "Index must be between 0 and " + value.length() );
            }
        }

        @Override
        public int size()
        {
            return stringValue().length();
        }

    }

    public static class ListValue extends PackValue
    {

        private static final ListValue EMPTY_LIST = new ListValue( EMPTY_LIST_OF_VALUES );

        public static ListValue getInstance( List<PackValue> values )
        {
            if ( values.isEmpty() )
            {
                return EMPTY_LIST;
            }
            else
            {
                return new ListValue( values );
            }
        }

        private final List<PackValue> values;

        private ListValue( List<PackValue> values ) { this.values = values; }

        @Override
        public boolean isList() { return true; }

        @Override
        public boolean booleanValue() { return !values.isEmpty(); }

        @Override
        public int intValue() { return 0; }

        @Override
        public long longValue() { return 0; }

        @Override
        public float floatValue() { return 0; }

        @Override
        public double doubleValue() { return 0; }

        @Override
        public byte[] byteArrayValue() { return null; }

        @Override
        public String stringValue() { return null; }

        @Override
        public List<PackValue> listValue() { return values; }

        @Override
        public Iterator<PackValue> iterator() { return values.iterator(); }

        @Override
        public PackValue get( int index )
        {
            if ( index >= 0 && index < values.size() )
            {
                return values.get( index );
            }
            else
            {
                throw new ArrayIndexOutOfBoundsException(
                        "Index must be between 0 and " + values.size() );
            }
        }

        @Override
        public int size()
        {
            return values.size();
        }

    }

    public static class MapValue extends PackValue
    {

        private static final MapValue EMPTY_MAP = new MapValue( EMPTY_MAP_OF_VALUES );

        public static MapValue getInstance( Map<String, PackValue> values )
        {
            if ( values.isEmpty() )
            {
                return EMPTY_MAP;
            }
            else
            {
                return new MapValue( values );
            }
        }

        private final Map<String,PackValue> values;

        private MapValue( Map<String,PackValue> values ) { this.values = values; }

        @Override
        public boolean isMap() { return true; }

        @Override
        public boolean booleanValue() { return !values.isEmpty(); }

        @Override
        public int intValue() { return 0; }

        @Override
        public long longValue() { return 0; }

        @Override
        public float floatValue() { return 0; }

        @Override
        public double doubleValue() { return 0; }

        @Override
        public byte[] byteArrayValue() { return null; }

        @Override
        public String stringValue() { return null; }

        @Override
        public Map<String,PackValue> mapValue() { return values; }

        @Override
        public PackValue get( String key )
        {
            if ( values.containsKey( key ) )
            {
                return values.get( key );
            }
            else
            {
                return null;
            }
        }

        @Override
        public int size()
        {
            return values.size();
        }

    }

    public static class StructValue extends PackValue
    {

        public static StructValue getInstance( char signature, List<PackValue> values )
        {
            return new StructValue( signature, values );
        }

        private final char signature;
        private final List<PackValue> values;

        private StructValue( char signature, List<PackValue> values )
        {
            this.signature = signature;
            this.values = values;
        }

        @Override
        public boolean isStruct() { return true; }

        @Override
        public boolean booleanValue() { return !values.isEmpty(); }

        @Override
        public int intValue() { return 0; }

        @Override
        public long longValue() { return 0; }

        @Override
        public float floatValue() { return 0; }

        @Override
        public double doubleValue() { return 0; }

        @Override
        public byte[] byteArrayValue() { return null; }

        @Override
        public String stringValue() { return null; }

        @Override
        public List<PackValue> listValue() { return values; }

        @Override
        public Iterator<PackValue> iterator() { return values.iterator(); }

        @Override
        public PackValue get( int index )
        {
            if ( index >= 0 && index < values.size() )
            {
                return values.get( index );
            }
            else
            {
                throw new ArrayIndexOutOfBoundsException(
                        "Index must be between 0 and " + values.size() );
            }
        }

        @Override
        public int size()
        {
            return values.size();
        }

        @Override
        public char signature()
        {
            return signature;
        }

    }

}