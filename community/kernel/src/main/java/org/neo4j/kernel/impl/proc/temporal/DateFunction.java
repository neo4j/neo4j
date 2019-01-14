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
package org.neo4j.kernel.impl.proc.temporal;

import java.time.Clock;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;

import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDate;

@Description( "Create a Date instant." )
class DateFunction extends TemporalFunction<DateValue>
{
    DateFunction( Supplier<ZoneId> defaultZone )
    {
        super( NTDate, defaultZone );
    }

    @Override
    protected DateValue now( Clock clock, String timezone, Supplier<ZoneId> defaultZone )
    {
        return timezone == null ? DateValue.now( clock, defaultZone ) : DateValue.now( clock, timezone );
    }

    @Override
    protected DateValue parse( TextValue value, Supplier<ZoneId> defaultZone )
    {
        return DateValue.parse( value );
    }

    @Override
    protected DateValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return DateValue.build( map, defaultZone );
    }

    @Override
    protected DateValue select( AnyValue from, Supplier<ZoneId> defaultZone )
    {
        return DateValue.select( from, defaultZone );
    }

    @Override
    protected DateValue truncate( TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone )
    {
        return DateValue.truncate( unit, input, fields, defaultZone );
    }
}
