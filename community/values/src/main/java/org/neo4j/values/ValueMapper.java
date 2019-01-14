/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.values;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.CharArray;
import org.neo4j.values.storable.CharValue;
import org.neo4j.values.storable.DateArray;
import org.neo4j.values.storable.DateTimeArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationArray;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.StringArray;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public interface ValueMapper<Base>
{
    // Virtual

    Base mapPath( PathValue value );

    Base mapNode( VirtualNodeValue value );

    Base mapRelationship( VirtualRelationshipValue value );

    Base mapMap( MapValue value );

    // Storable

    Base mapNoValue();

    Base mapSequence( SequenceValue value );

    Base mapText( TextValue value );

    default Base mapString( StringValue value )
    {
        return mapText( value );
    }

    default Base mapTextArray( TextArray value )
    {
        return mapSequence( value );
    }

    default Base mapStringArray( StringArray value )
    {
        return mapTextArray( value );
    }

    default Base mapChar( CharValue value )
    {
        return mapText( value );
    }

    default Base mapCharArray( CharArray value )
    {
        return mapTextArray( value );
    }

    Base mapBoolean( BooleanValue value );

    default Base mapBooleanArray( BooleanArray value )
    {
        return mapSequence( value );
    }

    Base mapNumber( NumberValue value );

    default Base mapNumberArray( NumberArray value )
    {
        return mapSequence( value );
    }

    default Base mapIntegral( IntegralValue value )
    {
        return mapNumber( value );
    }

    default Base mapIntegralArray( IntegralArray value )
    {
        return mapNumberArray( value );
    }

    default Base mapByte( ByteValue value )
    {
        return mapIntegral( value );
    }

    default Base mapByteArray( ByteArray value )
    {
        return mapIntegralArray( value );
    }

    default Base mapShort( ShortValue value )
    {
        return mapIntegral( value );
    }

    default Base mapShortArray( ShortArray value )
    {
        return mapIntegralArray( value );
    }

    default Base mapInt( IntValue value )
    {
        return mapIntegral( value );
    }

    default Base mapIntArray( IntArray value )
    {
        return mapIntegralArray( value );
    }

    default Base mapLong( LongValue value )
    {
        return mapIntegral( value );
    }

    default Base mapLongArray( LongArray value )
    {
        return mapIntegralArray( value );
    }

    default Base mapFloatingPoint( FloatingPointValue value )
    {
        return mapNumber( value );
    }

    default Base mapFloatingPointArray( FloatingPointArray value )
    {
        return mapNumberArray( value );
    }

    default Base mapDouble( DoubleValue value )
    {
        return mapFloatingPoint( value );
    }

    default Base mapDoubleArray( DoubleArray value )
    {
        return mapFloatingPointArray( value );
    }

    default Base mapFloat( FloatValue value )
    {
        return mapFloatingPoint( value );
    }

    default Base mapFloatArray( FloatArray value )
    {
        return mapFloatingPointArray( value );
    }

    Base mapDateTime( DateTimeValue value );

    Base mapLocalDateTime( LocalDateTimeValue value );

    Base mapDate( DateValue value );

    Base mapTime( TimeValue value );

    Base mapLocalTime( LocalTimeValue value );

    Base mapDuration( DurationValue value );

    Base mapPoint( PointValue value );

    default Base mapPointArray( PointArray value )
    {
        return mapSequence( value );
    }

    default Base mapDateTimeArray( DateTimeArray value )
    {
        return mapSequence( value );
    }

    default Base mapLocalDateTimeArray( LocalDateTimeArray value )
    {
        return mapSequence( value );
    }

    default Base mapLocalTimeArray( LocalTimeArray value )
    {
        return mapSequence( value );
    }

    default Base mapTimeArray( TimeArray value )
    {
        return mapSequence( value );
    }

    default Base mapDateArray( DateArray value )
    {
        return mapSequence( value );
    }

    default Base mapDurationArray( DurationArray value )
    {
        return mapSequence( value );
    }

    abstract class JavaMapper implements ValueMapper<Object>
    {
        @Override
        public Object mapNoValue()
        {
            return null;
        }

        @Override
        public Object mapMap( MapValue value )
        {
            Map<Object,Object> map = new HashMap<>();
            value.foreach( ( k, v ) -> map.put( k, v.map( this ) ) );
            return map;
        }

        @Override
        public List<?> mapSequence( SequenceValue value )
        {
            List<Object> list = new ArrayList<>( value.length() );
            value.forEach( v -> list.add( v.map( this ) ) );
            return list;
        }

        @Override
        public Character mapChar( CharValue value )
        {
            return value.value();
        }

        @Override
        public String mapText( TextValue value )
        {
            return value.stringValue();
        }

        @Override
        public String[] mapStringArray( StringArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public char[] mapCharArray( CharArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public Boolean mapBoolean( BooleanValue value )
        {
            return value.booleanValue();
        }

        @Override
        public boolean[] mapBooleanArray( BooleanArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public Number mapNumber( NumberValue value )
        {
            return value.asObject();
        }

        @Override
        public byte[] mapByteArray( ByteArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public short[] mapShortArray( ShortArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public int[] mapIntArray( IntArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public long[] mapLongArray( LongArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public float[] mapFloatArray( FloatArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public double[] mapDoubleArray( DoubleArray value )
        {
            return value.asObjectCopy();
        }

        @Override
        public ZonedDateTime mapDateTime( DateTimeValue value )
        {
            return value.asObjectCopy();
        }

        @Override
        public LocalDateTime mapLocalDateTime( LocalDateTimeValue value )
        {
            return value.asObjectCopy();
        }

        @Override
        public LocalDate mapDate( DateValue value )
        {
            return value.asObjectCopy();
        }

        @Override
        public OffsetTime mapTime( TimeValue value )
        {
            return value.asObjectCopy();
        }

        @Override
        public LocalTime mapLocalTime( LocalTimeValue value )
        {
            return value.asObjectCopy();
        }

        @Override
        public TemporalAmount mapDuration( DurationValue value )
        {
            return value.asObjectCopy();
        }

        @Override
        public Point mapPoint( PointValue value )
        {
            return value.asObjectCopy();
        }
    }
}
