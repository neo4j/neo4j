/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphdb.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Map.entry;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.stringValue;

public class IndexSettingUtil
{
    @SuppressWarnings( "unchecked" )
    private static final Map<String,IndexSetting> INDEX_SETTING_REVERSE_LOOKUP = Map.ofEntries(
            Stream.of( IndexSettingImpl.values() ).map( s -> entry( s.getSettingName(), s ) ).toArray( Map.Entry[]::new ) );

    /**
     * @param string Case sensitive setting name.
     * @return Corresponding {@link IndexSettingImpl} or null if no match.
     */
    public static IndexSetting fromString( String string )
    {
        return INDEX_SETTING_REVERSE_LOOKUP.get( string );
    }

    public static IndexConfig toIndexConfigFromIndexSettingObjectMap( Map<IndexSetting,Object> indexConfiguration )
    {
        Map<String,Value> collectingMap = new HashMap<>();
        for ( Map.Entry<IndexSetting,Object> entry : indexConfiguration.entrySet() )
        {
            IndexSetting setting = entry.getKey();
            final Value value = asIndexSettingValue( setting, entry.getValue() );
            collectingMap.put( setting.getSettingName(), value );
        }
        return IndexConfig.with( collectingMap );
    }

    public static IndexConfig toIndexConfigFromStringObjectMap( Map<String,Object> configMap )
    {
        Map<IndexSetting,Object> collectingMap = new HashMap<>();
        for ( Map.Entry<String,Object> entry : configMap.entrySet() )
        {
            final String key = entry.getKey();
            final IndexSetting indexSetting = asIndexSetting( key );
            collectingMap.put( indexSetting, entry.getValue() );
        }
        return toIndexConfigFromIndexSettingObjectMap( collectingMap );
    }

    private static IndexSetting asIndexSetting( String key )
    {
        final IndexSetting indexSetting = fromString( key );
        if ( indexSetting == null )
        {
            throw new IllegalArgumentException( String.format( "Invalid index config key '%s', it was not recognized as an index setting.", key ) );
        }
        return indexSetting;
    }

    @VisibleForTesting
    static Value asIndexSettingValue( IndexSetting setting, Object value )
    {
        Objects.requireNonNull( value, "Index setting value can not be null." );
        return parse( setting, value );
    }

    private static Value parse( IndexSetting indexSetting, Object value )
    {
        final Class<?> type = indexSetting.getType();
        if ( type == Boolean.class )
        {
            return parseAsBoolean( value );
        }
        if ( type == double[].class )
        {
            return parseAsDoubleArray( value );
        }
        if ( type == String.class )
        {
            return stringValue( value.toString() );
        }
        if ( type == Integer.class )
        {
            return parseAsInteger( value );
        }
        throw new UnsupportedOperationException(
                "Should not happen. Missing parser for type " + type.getSimpleName() + ". This type is used by indexSetting " + indexSetting.getSettingName() );
    }

    private static IntValue parseAsInteger( Object value )
    {
        if ( value instanceof Number )
        {
            return Values.intValue( ((Number) value).intValue() );
        }
        throw new IllegalArgumentException( "Could not parse value '" + value + "' of type " + value.getClass().getSimpleName() + " as integer." );
    }

    private static DoubleArray parseAsDoubleArray( Object value )
    {
        // Primitive arrays
        if ( value instanceof byte[] )
        {
            final double[] doubleArray = toDoubleArray( (byte[]) value );
            return doubleArray( doubleArray );
        }
        if ( value instanceof short[] )
        {
            final double[] doubleArray = toDoubleArray( (short[]) value );
            return doubleArray( doubleArray );
        }
        if ( value instanceof int[] )
        {
            final double[] doubleArray = toDoubleArray( (int[]) value );
            return doubleArray( doubleArray );
        }
        if ( value instanceof long[] )
        {
            final double[] doubleArray = toDoubleArray( (long[]) value );
            return doubleArray( doubleArray );
        }
        if ( value instanceof float[] )
        {
            final double[] doubleArray = toDoubleArray( (float[]) value );
            return doubleArray( doubleArray );
        }
        if ( value instanceof double[] )
        {
            return doubleArray( (double[]) value );
        }

        // Non primitive arrays
        if ( value instanceof Number[] )
        {
            final Number[] numberArray = (Number[]) value;
            final double[] doubleArray = new double[numberArray.length];
            for ( int i = 0; i < numberArray.length; i++ )
            {
                doubleArray[i] = numberArray[i].doubleValue();
            }
            return doubleArray( doubleArray );
        }

        // Collection
        if ( value instanceof Collection )
        {
            final Collection collection = (Collection) value;
            final double[] doubleArray = new double[collection.size()];
            final Iterator iterator = collection.iterator();
            for ( int i = 0; iterator.hasNext(); i++ )
            {
                final Object next = iterator.next();
                if ( next instanceof Number )
                {
                    doubleArray[i] = ((Number) next).doubleValue();
                }
                else
                {
                    throw new IllegalArgumentException(
                            "Could not parse value '" + value + "' of type " + next.getClass().getSimpleName() + " as double." );
                }
            }
            return doubleArray( doubleArray );
        }

        throw new IllegalArgumentException( "Could not parse value '" + value + "' as double[]." );
    }

    private static BooleanValue parseAsBoolean( Object value )
    {
        if ( value instanceof Boolean )
        {
            return booleanValue( (Boolean) value );
        }
        throw new IllegalArgumentException( "Could not parse value '" + value + "' as boolean." );
    }

    private static double[] toDoubleArray( byte[] value )
    {
        final double[] doubleArray = new double[value.length];
        for ( int i = 0; i < value.length; i++ )
        {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray( short[] value )
    {
        final double[] doubleArray = new double[value.length];
        for ( int i = 0; i < value.length; i++ )
        {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray( int[] value )
    {
        final double[] doubleArray = new double[value.length];
        for ( int i = 0; i < value.length; i++ )
        {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray( long[] value )
    {
        final double[] doubleArray = new double[value.length];
        for ( int i = 0; i < value.length; i++ )
        {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray( float[] value )
    {
        final double[] doubleArray = new double[value.length];
        for ( int i = 0; i < value.length; i++ )
        {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }
}
