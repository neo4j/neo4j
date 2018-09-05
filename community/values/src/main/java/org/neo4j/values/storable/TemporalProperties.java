/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.values.storable;

import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;

public enum TemporalProperties
{
    MILLENNIUM( "millennium", ChronoUnit.MILLENNIA ),
    CENTURY( "century", ChronoUnit.CENTURIES ),
    DECADE( "decade", ChronoUnit.DECADES ),
    YEAR( "year", ChronoUnit.YEARS ),
    WEEK_YEAR( "weekYear", IsoFields.WEEK_BASED_YEARS ),
    QUARTER( "quarter", IsoFields.QUARTER_YEARS ),
    MONTH( "month", ChronoUnit.MONTHS ),
    WEEK( "week", ChronoUnit.WEEKS ),
    DAY( "day", ChronoUnit.DAYS ),
    HOUR( "hour", ChronoUnit.HOURS ),
    MINUTE( "minute", ChronoUnit.MINUTES ),
    SECOND( "second", ChronoUnit.SECONDS ),
    MILLISECOND( "millisecond", ChronoUnit.MILLIS ),
    MICROSECOND( "microsecond", ChronoUnit.MICROS );


    public String propertyKey;
    public TemporalUnit unit;

    TemporalProperties( String propertyKey, TemporalUnit unit )
    {
        this.propertyKey = propertyKey;
        this.unit = unit;
    }

    public static TemporalProperties fromName( String unit )
    {
        switch ( unit )
        {
        case "millennium":
            return MILLENNIUM;
        case "century":
            return CENTURY;
        case "decade":
            return DECADE;
        case "year":
            return YEAR;
        case "weekYear":
            return WEEK_YEAR;
        case "quarter":
            return QUARTER;
        case "month":
            return MONTH;
        case "week":
            return WEEK;
        case "day":
            return DAY;
        case "hour":
            return HOUR;
        case "minute":
            return MINUTE;
        case "second":
            return SECOND;
        case "millisecond":
            return MILLISECOND;
        case "microsecond":
            return MICROSECOND;
        default:
            throw new IllegalArgumentException( "Unsupported unit: " + unit );
        }
    }
}
