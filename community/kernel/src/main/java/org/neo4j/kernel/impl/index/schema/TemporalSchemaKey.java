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
package org.neo4j.kernel.impl.index.schema;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

class TemporalSchemaKey extends NativeSchemaKey<TemporalSchemaKey>
{
    private static final int[] SIZES = new int[]{8 + 9, 28 + 9, 12 + 9, 8 + 9, 18 + 9, 12 + 9};

    /**
     * An average month is 30 days, 10 hours and 30 minutes.
     * In seconds this is (((30 * 24) + 10) * 60 + 30) * 60 = 2629800
     */
    private static final long AVG_MONTH_SECONDS = 2_629_800;
    private static final long AVG_DAY_SECONDS = 86_400;

    // var1: long epochDay;
    static final byte TYPE_DATE = 0;

    // var1: long totalAvgSeconds;
    // var2: int nanosOfSecond;
    // var3: long months;
    // var4: long days;
    static final byte TYPE_DURATION = 1;

    // var1: long epochSecond;
    // var2: int nanoOfSecond;
    static final byte TYPE_LOCAL_DATE_TIME = 2;

    // var1: long nanoOfDay;
    static final byte TYPE_LOCAL_TIME = 3;

    // var1: long epochSecondUTC;
    // var2: int nanoOfSecond;
    // var3: short zoneId;
    // var4: int zoneOffsetSeconds;
    static final byte TYPE_ZONED_DATE_TIME = 4;

    // var1: long nanosOfDayUTC;
    // var2: int zoneOffsetSeconds;
    static final byte TYPE_ZONED_TIME = 5;

    byte type;
    long var1;
    long var2;
    long var3;
    long var4;

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( value.valueGroup().category() != ValueCategory.TEMPORAL )
        {
            throw new IllegalArgumentException( "Key layout does only support temporal values, tried to create key from " + value );
        }
        return value;
    }

    @Override
    Value asValue()
    {
        switch ( type )
        {
        case TYPE_DATE:
            return DateValue.epochDate( var1 );
        case TYPE_DURATION:
            long seconds = var1 - var3 * AVG_MONTH_SECONDS - var4 * AVG_DAY_SECONDS;
            return DurationValue.duration( var3, var4, seconds, var2 );
        case TYPE_LOCAL_DATE_TIME:
            return LocalDateTimeValue.localDateTime( var1, var2 );
        case TYPE_LOCAL_TIME:
            return LocalTimeValue.localTime( var1 );
        case TYPE_ZONED_DATE_TIME:
            return TimeZones.validZoneId( (short) var3 ) ? DateTimeValue.datetime( var1, (int) var2, ZoneId.of( TimeZones.map( (short) var3 ) ) )
                                                         : DateTimeValue.datetime( var1, (int) var2, ZoneOffset.ofTotalSeconds( (int) var4 ) );
        case TYPE_ZONED_TIME:
            // We need to check validity upfront without throwing exceptions, because the PageCursor might give garbage bytes
            if ( TimeZones.validZoneOffset( (int) var2 ) )
            {
                return TimeValue.time( var1, ZoneOffset.ofTotalSeconds( (int) var2 ) );
            }
            return NO_VALUE;
        default:
            throw new IllegalArgumentException( "Unexpected type " + type );
        }
    }

    @Override
    void initValueAsLowest()
    {
        type = Byte.MIN_VALUE;
        var1 = Long.MIN_VALUE;
        var2 = Long.MIN_VALUE;
        var3 = Long.MIN_VALUE;
        var4 = Long.MIN_VALUE;
    }

    @Override
    void initValueAsHighest()
    {
        type = Byte.MAX_VALUE;
        var1 = Long.MAX_VALUE;
        var2 = Long.MAX_VALUE;
        var3 = Long.MAX_VALUE;
        var4 = Long.MAX_VALUE;
    }

    @Override
    public void writeDate( long epochDay )
    {
        this.type = TYPE_DATE;
        this.var1 = epochDay;
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos )
    {
        this.type = TYPE_DURATION;
        this.var1 = months * AVG_MONTH_SECONDS + days * AVG_DAY_SECONDS + seconds;
        this.var2 = nanos;
        this.var3 = months;
        this.var4 = days;
    }

    @Override
    public void writeLocalDateTime( long epochSecond, int nano )
    {
        this.type = TYPE_LOCAL_DATE_TIME;
        this.var1 = epochSecond;
        this.var2 = nano;
    }

    @Override
    public void writeLocalTime( long nanoOfDay )
    {
        this.type = TYPE_LOCAL_TIME;
        this.var1 = nanoOfDay;
    }

    @Override
    public void writeDateTime( long epochSecondUTC, int nano, int offsetSeconds )
    {
        this.type = TYPE_ZONED_DATE_TIME;
        this.var1 = epochSecondUTC;
        this.var2 = nano;
        this.var3 = -1;
        this.var4 = offsetSeconds;
    }

    @Override
    public void writeDateTime( long epochSecondUTC, int nano, String zoneId )
    {
        this.type = TYPE_ZONED_DATE_TIME;
        this.var1 = epochSecondUTC;
        this.var2 = nano;
        this.var3 = TimeZones.map( zoneId );
        this.var4 = 0;
    }

    @Override
    public void writeTime( long nanosOfDayUTC, int offsetSeconds )
    {
        this.type = TYPE_ZONED_TIME;
        this.var1 = nanosOfDayUTC;
        this.var2 = offsetSeconds;
    }

    @Override
    int compareValueTo( TemporalSchemaKey other )
    {
        int typeComparison = Byte.compare( type, other.type );
        if ( typeComparison != 0 )
        {
            return typeComparison;
        }

        int comparison;
        switch ( type )
        {
        case TYPE_DATE:
            return Long.compare( var1, other.var1 );
        case TYPE_DURATION:
            comparison = Long.compare( var1, other.var1 );
            if ( comparison == 0 )
            {
                comparison = Integer.compare( (int) var2, (int) other.var2 );
                if ( comparison == 0 )
                {
                    comparison = Long.compare( var3, other.var3 );
                    if ( comparison == 0 )
                    {
                        comparison = Long.compare( var4, other.var4 );
                    }
                }
            }
            return comparison;
        case TYPE_LOCAL_DATE_TIME:
            comparison = Long.compare( var1, other.var1 );
            if ( comparison == 0 )
            {
                comparison = Integer.compare( (int) var2, (int) other.var2 );
            }
            return comparison;
        case TYPE_LOCAL_TIME:
            return Long.compare( var1, other.var1 );
        case TYPE_ZONED_DATE_TIME:
            comparison = Long.compare( var1, other.var1 );
            if ( comparison == 0 )
            {
                comparison = Integer.compare( (int) var2, (int) other.var2 );
                if ( comparison == 0 &&
                        // We need to check validity upfront without throwing exceptions, because the PageCursor might give garbage bytes
                        TimeZones.validZoneOffset( (int) var4 ) && TimeZones.validZoneOffset( (int) other.var4 ) )
                {
                    // In the rare case of comparing the same instant in different time zones, we settle for
                    // mapping to values and comparing using the general values comparator.
                    comparison = Values.COMPARATOR.compare( asValue(), other.asValue() );
                }
            }
            return comparison;
        case TYPE_ZONED_TIME:
            comparison = Long.compare( var1, other.var1 );
            if ( comparison == 0 )
            {
                comparison = Integer.compare( (int) var2, (int) other.var2 );
            }
            return comparison;
        default:
            return 0;
        }
    }

    void copyFrom( TemporalSchemaKey key )
    {
        this.type = key.type;
        this.var1 = key.var1;
        this.var2 = key.var2;
        this.var3 = key.var3;
        this.var4 = key.var4;
        setEntityId( key.getEntityId() );
        setCompareId( key.getCompareId() );
    }

    int size()
    {
        return SIZES[type];
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,type=%d",
                asValue(),
                getEntityId(),
                type );
    }
}
