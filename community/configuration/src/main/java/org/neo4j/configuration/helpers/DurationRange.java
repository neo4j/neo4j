/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration.helpers;

import java.time.Duration;
import java.util.Objects;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.internal.helpers.TimeUtil;

/**
 * Duration Range derived from configuration.
 * This class wraps two durations, which are non-negative and maximum is not longer than minimum.
 */
@PublicApi
public class DurationRange
{
    private Duration min;
    private Duration max;

    public DurationRange( Duration min, Duration max )
    {
        this.min = min;
        this.max = max;
        if ( min.toNanos() > max.toNanos() )
        {
            throw new IllegalArgumentException( "min may not be longer than max" );
        }
    }

    public static DurationRange fromSeconds( int min, int max )
    {
        return new DurationRange( Duration.ofSeconds( min ), Duration.ofSeconds( max ) );
    }

    public static DurationRange parse( String value )
    {
        var parts = value.trim()
                         .replaceAll( "^\\[", "" )
                         .replaceAll( "\\]$", "" )
                         .split( "-" );
        if ( parts.length != 2 ||
             parts[0].isEmpty() ||
             parts[1].isEmpty() )
        {
            throw new IllegalArgumentException( "must be in format <min>-<max>, where min and max are non-negative durations" );
        }
        var min = Duration.ofMillis( TimeUtil.parseTimeMillis.apply( parts[0] ) );
        var max = Duration.ofMillis( TimeUtil.parseTimeMillis.apply( parts[1] ) );
        return new DurationRange( min, max );
    }

    public Duration getMin()
    {
        return min;
    }

    public Duration getMax()
    {
        return max;
    }

    /**
     * @return returns the difference between minimum and maximum
     */
    public Duration getDelta()
    {
        return max.minus( min );
    }

    public String valueToString()
    {
        return TimeUtil.nanosToString( min.toNanos() ) + '-' + TimeUtil.nanosToString( max.toNanos() );
    }

    @Override
    public String toString()
    {
        return '[' + min.toString() + '-' + max + ']';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DurationRange that = (DurationRange) o;
        return min.equals( that.min ) &&
               max.equals( that.max );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( min, max );
    }
}
