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
package org.neo4j.kernel.impl.proc.temporal;

import java.time.Clock;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;

import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.proc.Neo4jTypes.NTDateTime;

@Description( "Create a DateTime instant." )
class DateTimeFunction extends TemporalFunction<DateTimeValue>
{
    DateTimeFunction()
    {
        super( NTDateTime );
    }

    @Override
    protected DateTimeValue now( Clock clock, String timezone )
    {
        return timezone == null ? DateTimeValue.now( clock ) : DateTimeValue.now( clock, timezone );
    }

    @Override
    protected DateTimeValue parse( TextValue value, Supplier<ZoneId> defaultZone )
    {
        return DateTimeValue.parse( value, defaultZone );
    }

    @Override
    protected DateTimeValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return DateTimeValue.build( map, defaultZone );
    }

    @Override
    protected DateTimeValue positionalCreate( AnyValue[] input )
    {
        if ( input.length != 8 )
        {
            throw new IllegalArgumentException( "expected 8 arguments" );
        }
        return DateTimeValue.datetime(
                anInt( "year", input[0] ),
                anInt( "month", input[1] ),
                anInt( "day", input[2] ),
                anInt( "hour", input[3] ),
                anInt( "minute", input[4] ),
                anInt( "second", input[5] ),
                anInt( "nanos", input[6] ),
                aString( "timezone", input[7] ) );
    }

    @Override
    protected DateTimeValue truncate( TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone )
    {
        return DateTimeValue.truncate( unit, input, fields, defaultZone );
    }
}
