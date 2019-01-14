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
package org.neo4j.kernel.impl.store;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.utils.TemporalUtil;

import static java.time.ZoneOffset.UTC;

/**
 * A {@link ValueWriter} that defines format for all temporal types, except duration.
 * Subclasses will not be able to override methods like {@link #writeDate(LocalDate)}. They should instead override {@link #writeDate(long)} that
 * defines how {@link LocalDate} is serialized.
 * <p>
 * Primary purpose of this class is to share serialization format between property store writer and schema indexes.
 *
 * @param <E> the error type.
 */
public abstract class TemporalValueWriterAdapter<E extends Exception> extends ValueWriter.Adapter<E>
{
    @Override
    public final void writeDate( LocalDate localDate ) throws E
    {
        writeDate( localDate.toEpochDay() );
    }

    @Override
    public final void writeLocalTime( LocalTime localTime ) throws E
    {
        writeLocalTime( localTime.toNanoOfDay() );
    }

    @Override
    public final void writeTime( OffsetTime offsetTime ) throws E
    {
        long nanosOfDayUTC = TemporalUtil.getNanosOfDayUTC( offsetTime );
        int offsetSeconds = offsetTime.getOffset().getTotalSeconds();
        writeTime( nanosOfDayUTC, offsetSeconds );
    }

    @Override
    public final void writeLocalDateTime( LocalDateTime localDateTime ) throws E
    {
        long epochSecond = localDateTime.toEpochSecond( UTC );
        int nano = localDateTime.getNano();
        writeLocalDateTime( epochSecond, nano );
    }

    @Override
    public final void writeDateTime( ZonedDateTime zonedDateTime ) throws E
    {
        long epochSecondUTC = zonedDateTime.toEpochSecond();
        int nano = zonedDateTime.getNano();

        ZoneId zone = zonedDateTime.getZone();
        if ( zone instanceof ZoneOffset )
        {
            int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();
            writeDateTime( epochSecondUTC, nano, offsetSeconds );
        }
        else
        {
            String zoneId = zone.getId();
            writeDateTime( epochSecondUTC, nano, zoneId );
        }
    }

    /**
     * Write date value obtained from {@link LocalDate} in {@link #writeDate(LocalDate)}.
     *
     * @param epochDay the epoch day.
     */
    protected void writeDate( long epochDay ) throws E
    {
    }

    /**
     * Write local time value obtained from {@link LocalTime} in {@link #writeLocalTime(LocalTime)}.
     *
     * @param nanoOfDay the nanosecond of the day.
     */
    protected void writeLocalTime( long nanoOfDay ) throws E
    {
    }

    /**
     * Write time value obtained from {@link OffsetTime} in {@link #writeTime(OffsetTime)}.
     *
     * @param nanosOfDayUTC nanoseconds of day in UTC. will be between -18h and +42h
     * @param offsetSeconds time zone offset in seconds
     */
    protected void writeTime( long nanosOfDayUTC, int offsetSeconds ) throws E
    {
    }

    /**
     * Write local date-time value obtained from {@link LocalDateTime} in {@link #writeLocalDateTime(LocalDateTime)}.
     *
     * @param epochSecond the epoch second in UTC.
     * @param nano the nanosecond.
     */
    protected void writeLocalDateTime( long epochSecond, int nano ) throws E
    {
    }

    /**
     * Write zoned date-time value obtained from {@link ZonedDateTime} in {@link #writeDateTime(ZonedDateTime)}.
     *
     * @param epochSecondUTC the epoch second in UTC (no offset).
     * @param nano the nanosecond.
     * @param offsetSeconds the offset in seconds.
     */
    protected void writeDateTime( long epochSecondUTC, int nano, int offsetSeconds ) throws E
    {
    }

    /**
     * Write zoned date-time value obtained from {@link ZonedDateTime} in {@link #writeDateTime(ZonedDateTime)}.
     *
     * @param epochSecondUTC the epoch second in UTC (no offset).
     * @param nano the nanosecond.
     * @param zoneId the timezone id.
     */
    protected void writeDateTime( long epochSecondUTC, int nano, String zoneId ) throws E
    {
    }
}
