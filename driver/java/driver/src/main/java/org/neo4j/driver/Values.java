/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IdentityValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.TextValue;

/**
 * Utility for wrapping regular Java types and exposing them as {@link org.neo4j.driver.Value}
 * objects.
 */
public class Values
{
    public static final Value EmptyMap = value( Collections.EMPTY_MAP );

    public static Value value( Object value )
    {
        if ( value == null ) { return null; }
        if ( value instanceof Value ) { return (Value) value; }

        if ( value instanceof Identity ) { return new IdentityValue( (Identity) value ); }
        if ( value instanceof Node ) { return new NodeValue( (Node) value ); }
        if ( value instanceof Relationship ) { return new RelationshipValue( (Relationship) value ); }
        if ( value instanceof Path ) { return new PathValue( (Path) value ); }

        if ( value instanceof Map ) { return value( (Map<String,Object>) value ); }
        if ( value instanceof Collection ) { return value( (List<Object>) value ); }
        if ( value instanceof Iterable ) { return value( (Iterable<Object>) value ); }

        if ( value instanceof String ) { return value( (String) value ); }
        if ( value instanceof Boolean ) { return value( (boolean) value ); }
        if ( value instanceof Byte ) { return value( (byte) value ); }
        if ( value instanceof Character ) { return value( (char) value ); }
        if ( value instanceof Short ) { return value( (short) value ); }
        if ( value instanceof Integer ) { return value( (int) value ); }
        if ( value instanceof Long ) { return value( (long) value ); }
        if ( value instanceof Float ) { return value( (float) value ); }
        if ( value instanceof Double ) { return value( (double) value ); }

        if ( value instanceof String[] ) { return value( (String[]) value ); }
        if ( value instanceof boolean[] ) { return value( (boolean[]) value ); }
        if ( value instanceof char[] ) { return value( (char[]) value ); }
        if ( value instanceof short[] ) { return value( (short[]) value ); }
        if ( value instanceof int[] ) { return value( (int[]) value ); }
        if ( value instanceof long[] ) { return value( (long[]) value ); }
        if ( value instanceof float[] ) { return value( (float[]) value ); }
        if ( value instanceof double[] ) { return value( (double[]) value ); }

        throw new ClientException( "Unable to convert " + value.getClass().getName() + " to Neo4j Value." );
    }

    public static Value value( short[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( int[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( long[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( boolean[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( float[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( double[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( char[] val )
    {
        return new TextValue( new String( val ) );
    }

    public static Value value( String[] val )
    {
        TextValue[] values = new TextValue[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = new TextValue( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( Object[] val )
    {
        Value[] values = new Value[val.length];
        for ( int i = 0; i < val.length; i++ )
        {
            values[i] = value( val[i] );
        }
        return new ListValue( values );
    }

    public static Value value( List<Object> val )
    {
        Value[] values = new Value[val.size()];
        for ( int i = 0; i < val.size(); i++ )
        {
            values[i] = value( val.get( i ) );
        }
        return new ListValue( values );
    }

    public static Value value( Iterable<Object> val )
    {
        List<Value> values = new ArrayList<>();
        for ( Object v : val )
        {
            values.add( value( v ) );
        }
        return new ListValue( values.toArray( new Value[values.size()] ) );
    }

    public static Value value( final long val )
    {
        return new IntegerValue( val );
    }

    public static Value value( final double val )
    {
        return new FloatValue( val );
    }

    public static Value value( final boolean val )
    {
        return new BooleanValue( val );
    }

    public static Value value( final char val )
    {
        return new TextValue( Character.toString( val ) );
    }

    public static Value value( final String val )
    {
        return new TextValue( val );
    }

    public static Value value( final Map<String,Object> val )
    {
        Map<String,Value> asValues = new HashMap<>( val.size() );
        for ( Map.Entry<String,Object> entry : val.entrySet() )
        {
            asValues.put( entry.getKey(), value( entry.getValue() ) );
        }
        return new MapValue( asValues );
    }

    public static Value[] values( final Object... vals )
    {
        Value[] values = new Value[vals.length];
        for ( int i = 0; i < vals.length; i++ )
        {
            values[i] = value( vals[i] );
        }
        return values;
    }

    /**
     * Helper function for creating a map of properties.
     *
     * @param keysAndValues alternating sequence of keys and values
     * @return Map containing all properties specified
     */
    public static Map<String,Value> properties( Object... keysAndValues )
    {
        HashMap<String,Value> map = new HashMap<>( keysAndValues.length / 2 );
        String key = null;
        for ( Object keyOrValue : keysAndValues )
        {
            if ( key == null )
            {
                key = keyOrValue.toString();
            }
            else
            {

                map.put( key, value( keyOrValue ) );
                key = null;
            }
        }
        return map;
    }

    public static Map<String,Value> toValueMap( Map<String,Object> input )
    {
        HashMap<String,Value> map = new HashMap<>( input.size() );
        for ( Map.Entry<String,Object> entry : input.entrySet() )
        {
            map.put( entry.getKey(), value( entry.getValue() ) );
        }
        return map;
    }

}
