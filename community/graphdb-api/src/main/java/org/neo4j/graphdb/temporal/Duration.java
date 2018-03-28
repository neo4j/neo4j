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
package org.neo4j.graphdb.temporal;

import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public interface Duration extends TemporalAmount
{
    long NANOS_PER_SECOND = 1_000_000_000L;
    long SECONDS_PER_DAY = DAYS.getDuration().getSeconds();
    List<TemporalUnit> UNITS = unmodifiableList( asList( MONTHS, DAYS, SECONDS, NANOS ) );

    @Override
    default List<TemporalUnit> getUnits()
    {
        return UNITS;
    }

    @Override
    default long get( TemporalUnit unit )
    {
        if ( unit instanceof ChronoUnit )
        {
            switch ( (ChronoUnit) unit )
            {
            case MONTHS:
                return getMonths();
            case DAYS:
                return getDays();
            case SECONDS:
                return getSeconds();
            case NANOS:
                return getNanos();
            default:
                break;
            }
        }
        throw new UnsupportedTemporalTypeException( "Unsupported unit: " + unit );
    }

    @Override
    default Temporal addTo( Temporal temporal )
    {
        if ( getMonths() != 0 && temporal.isSupported( MONTHS ) )
        {
            temporal = temporal.plus( getMonths(), MONTHS );
        }
        if ( getDays() != 0 && temporal.isSupported( DAYS ) )
        {
            temporal = temporal.plus( getDays(), DAYS );
        }
        if ( getSeconds() != 0 )
        {
            if ( temporal.isSupported( SECONDS ) )
            {
                temporal = temporal.plus( getSeconds(), SECONDS );
            }
            else
            {
                long asDays = getSeconds() / SECONDS_PER_DAY;
                if ( asDays != 0 )
                {
                    temporal = temporal.plus( asDays, DAYS );
                }
            }
        }
        if ( getNanos() != 0 && temporal.isSupported( NANOS ) )
        {
            temporal = temporal.plus( getNanos(), NANOS );
        }
        return temporal;
    }

    @Override
    default Temporal subtractFrom( Temporal temporal )
    {
        if ( getMonths() != 0 && temporal.isSupported( MONTHS ) )
        {
            temporal = temporal.minus( getMonths(), MONTHS );
        }
        if ( getDays() != 0 && temporal.isSupported( DAYS ) )
        {
            temporal = temporal.minus( getDays(), DAYS );
        }
        if ( getSeconds() != 0 )
        {
            if ( temporal.isSupported( SECONDS ) )
            {
                temporal = temporal.minus( getSeconds(), SECONDS );
            }
            else if ( temporal.isSupported( DAYS ) )
            {
                long asDays = getSeconds() / SECONDS_PER_DAY;
                if ( asDays != 0 )
                {
                    temporal = temporal.minus( asDays, DAYS );
                }
            }
        }
        if ( getNanos() != 0 && temporal.isSupported( NANOS ) )
        {
            temporal = temporal.minus( getNanos(), NANOS );
        }
        return temporal;
    }

    int getNanos();

    long getSeconds();

    long getDays();

    long getMonths();
}