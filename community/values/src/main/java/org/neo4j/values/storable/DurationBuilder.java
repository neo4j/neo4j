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
package org.neo4j.values.storable;

import org.neo4j.values.StructureBuilder;

abstract class DurationBuilder<Input, Result> implements StructureBuilder<Input,Result>
{
    private Input years;
    private Input months;
    private Input weeks;
    private Input days;
    private Input hours;
    private Input minutes;
    private Input seconds;
    private Input milliseconds;
    private Input microseconds;
    private Input nanoseconds;

    @Override
    public final StructureBuilder<Input,Result> add( String field, Input value )
    {
        switch ( field.toLowerCase() )
        {
        case "years":
            this.years = value;
            break;
        case "months":
            this.months = value;
            break;
        case "weeks":
            this.weeks = value;
            break;
        case "days":
            this.days = value;
            break;
        case "hours":
            this.hours = value;
            break;
        case "minutes":
            this.minutes = value;
            break;
        case "seconds":
            this.seconds = value;
            break;
        case "milliseconds":
            this.milliseconds = value;
            break;
        case "microseconds":
            this.microseconds = value;
            break;
        case "nanoseconds":
            this.nanoseconds = value;
            break;
        default:
            throw new IllegalStateException( "Unknown field: " + field );
        }
        return this;
    }

    @Override
    public final Result build()
    {
        return create(
                years,
                months,
                weeks,
                days,
                hours,
                minutes,
                seconds,
                milliseconds,
                microseconds,
                nanoseconds );
    }

    abstract Result create(
            Input years,
            Input months,
            Input weeks,
            Input days,
            Input hours,
            Input minutes,
            Input seconds,
            Input milliseconds,
            Input microseconds,
            Input nanoseconds );
}
