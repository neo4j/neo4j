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

import java.time.Year;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.time.temporal.ValueRange;
import java.util.Locale;

import static java.time.temporal.ChronoUnit.CENTURIES;
import static java.time.temporal.ChronoUnit.DECADES;
import static java.time.temporal.ChronoUnit.MILLENNIA;
import static java.time.temporal.ChronoUnit.YEARS;

enum Neo4JTemporalField implements TemporalField
{
    YEAR_OF_DECADE( "Year of decade", YEARS, DECADES, 10 ),
    YEAR_OF_CENTURY( "Year of century", YEARS, CENTURIES, 100 ),
    YEAR_OF_MILLENNIUM( "Millennium", YEARS, MILLENNIA, 1000 );

    private final String name;
    private final TemporalUnit baseUnit;
    private final TemporalUnit rangeUnit;
    private final int years;
    private final ValueRange range;

    Neo4JTemporalField( String name, TemporalUnit baseUnit, TemporalUnit rangeUnit, int years )
    {
        this.name = name;
        this.baseUnit = baseUnit;
        this.rangeUnit = rangeUnit;
        this.years = years;
        this.range = ValueRange.of( Year.MIN_VALUE / years, Year.MAX_VALUE / years );
    }

    @Override
    public String getDisplayName( Locale locale )
    {
        return name;
    }

    @Override
    public TemporalUnit getBaseUnit()
    {
        return baseUnit;
    }

    @Override
    public TemporalUnit getRangeUnit()
    {
        return rangeUnit;
    }

    @Override
    public ValueRange range()
    {
        return range;
    }

    @Override
    public boolean isDateBased()
    {
        return true;
    }

    @Override
    public boolean isTimeBased()
    {
        return false;
    }

    @Override
    public boolean isSupportedBy( TemporalAccessor temporal )
    {
        return false;
    }

    @Override
    public ValueRange rangeRefinedBy( TemporalAccessor temporal )
    {
        // Always identical
        return range();
    }

    @Override
    public long getFrom( TemporalAccessor temporal )
    {
        throw new UnsupportedOperationException( "Getting a " + this.name + " from temporal values is not supported." );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <R extends Temporal> R adjustInto( R temporal, long newValue )
    {
        int newVal = range.checkValidIntValue( newValue, this );
        int oldYear = temporal.get( ChronoField.YEAR );
        return (R) temporal.with( ChronoField.YEAR, (oldYear / years) * years + newVal )
                           .with( TemporalAdjusters.firstDayOfYear() );
    }

    @Override
    public String toString()
    {
        return name;
    }

}
