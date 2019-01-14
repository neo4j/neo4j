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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link DurationValue}.
 *
 * Durations are tricky, because exactly how long a duration is depends on the start date. We therefore sort them by
 * average total time in seconds, but keep the original months and days so we can reconstruct the value.
 */
class DurationSchemaKey extends NativeSchemaKey<DurationSchemaKey>
{
    /**
     * An average month is 30 days, 10 hours and 30 minutes.
     * In seconds this is (((30 * 24) + 10) * 60 + 30) * 60 = 2629800
     */
    private static final long AVG_MONTH_SECONDS = 2_629_800;
    private static final long AVG_DAY_SECONDS = 86_400;

    static final int SIZE =
            Long.BYTES +    /* totalAvgSeconds */
            Integer.BYTES + /* nanosOfSecond */
            Long.BYTES +    /* months */
            Long.BYTES +    /* days */
            Long.BYTES;     /* entityId */

    long totalAvgSeconds;
    int nanosOfSecond;
    long months;
    long days;

    @Override
    public Value asValue()
    {
        long seconds = totalAvgSeconds - months * AVG_MONTH_SECONDS - days * AVG_DAY_SECONDS;
        return DurationValue.duration( months, days, seconds, nanosOfSecond );
    }

    @Override
    public void initValueAsLowest()
    {
        totalAvgSeconds = Long.MIN_VALUE;
        nanosOfSecond = Integer.MIN_VALUE;
        months = Long.MIN_VALUE;
        days = Long.MIN_VALUE;
    }

    @Override
    public void initValueAsHighest()
    {
        totalAvgSeconds = Long.MAX_VALUE;
        nanosOfSecond = Integer.MAX_VALUE;
        months = Long.MAX_VALUE;
        days = Long.MAX_VALUE;
    }

    @Override
    public int compareValueTo( DurationSchemaKey other )
    {
        int comparison = Long.compare( totalAvgSeconds, other.totalAvgSeconds );
        if ( comparison == 0 )
        {
            comparison = Integer.compare( nanosOfSecond, other.nanosOfSecond );
            if ( comparison == 0 )
            {
                comparison = Long.compare( months, other.months );
                if ( comparison == 0 )
                {
                    comparison = Long.compare( days, other.days );
                }
            }
        }
        return comparison;
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,totalAvgSeconds=%d,nanosOfSecond=%d,months=%d,days=%d",
                        asValue(), getEntityId(), totalAvgSeconds, nanosOfSecond, months, days );
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos )
    {   // no-op
        this.totalAvgSeconds = months * AVG_MONTH_SECONDS + days * AVG_DAY_SECONDS + seconds;
        this.nanosOfSecond = nanos;
        this.months = months;
        this.days = days;
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !(value instanceof DurationValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support DurationValue, tried to create key from " + value );
        }
        return value;
    }
}
