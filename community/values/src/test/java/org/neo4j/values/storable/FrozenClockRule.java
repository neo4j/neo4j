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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalField;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static java.time.ZoneOffset.UTC;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class FrozenClockRule extends Clock implements TestRule, Function<String,Clock>, Supplier<ZoneId>
{
    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    @interface TimeZone
    {
        String[] value();
    }

    public static final TemporalAdjuster SECOND_PRECISION = temporal -> temporal.with( ChronoField.NANO_OF_SECOND, 0 );
    public static final TemporalAdjuster MILLISECOND_PRECISION = temporal -> temporal
            .with( ChronoField.NANO_OF_SECOND, temporal.get( ChronoField.MILLI_OF_SECOND ) * 1000_000 );
    private Instant instant;
    private ZoneId zone;

    @Override
    public ZoneId getZone()
    {
        return zone;
    }

    public Clock withZone( String zoneId )
    {
        return withZone( ZoneId.of( zoneId ) );
    }

    @Override
    public Clock withZone( ZoneId zone )
    {
        return fixed( instant, zone );
    }

    @Override
    public Instant instant()
    {
        return instant;
    }

    @Override
    public long millis()
    {
        return instant.toEpochMilli();
    }

    public Clock at( LocalDateTime datetime )
    {
        return at( datetime.atZone( zone ) );
    }

    public Clock at( OffsetDateTime datetime )
    {
        return fixed( datetime.toInstant(), datetime.getOffset() );
    }

    public Clock at( ZonedDateTime datetime )
    {
        return fixed( datetime.toInstant(), datetime.getZone() );
    }

    public Clock with( TemporalAdjuster adjuster )
    {
        return fixed( instant.with( adjuster ), zone );
    }

    public Clock with( TemporalField field, long newValue )
    {
        return fixed( instant.with( field, newValue ), zone );
    }

    @Override
    public Clock apply( String when )
    {
        return this;
    }

    @Override
    public ZoneId get()
    {
        return zone;
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    for ( ZoneId zoneId : zonesOf( description ) )
                    {
                        instant = Instant.now();
                        zone = zoneId;
                        base.evaluate();
                    }
                }
                finally
                {
                    instant = null;
                    zone = null;
                }
            }

            private ZoneId[] zonesOf( Description description )
            {
                TimeZone zone = description.getAnnotation( TimeZone.class );
                String[] ids = zone == null ? null : zone.value();
                if ( ids == null || ids.length == 0 )
                {
                    return new ZoneId[] {UTC};
                }
                else
                {
                    ZoneId[] zones = new ZoneId[ids.length];
                    for ( int i = 0; i < zones.length; i++ )
                    {
                        zones[i] = ZoneId.of( ids[i] );
                    }
                    return zones;
                }
            }
        };
    }

    static <V extends TemporalValue<?,V>> void assertEqualTemporal( V expected, V actual )
    {
        assertThat( actual, allOf(
                equalTo( expected ),
                equalOn( "timezone", FrozenClockRule::timezone, expected ),
                equalOn( "temporal", TemporalValue::temporal, expected ) ) );
    }

    private static ZoneId timezone( TemporalValue<?,?> temporal )
    {
        if ( temporal instanceof DateTimeValue )
        {
            return ((DateTimeValue) temporal).temporal().getZone();
        }
        if ( temporal instanceof TimeValue )
        {
            return ((TimeValue) temporal).temporal().getOffset();
        }
        return null;
    }

    private static <T, U> Matcher<T> equalOn( String trait, Function<T,U> mapping, T expected )
    {
        return new TypeSafeDiagnosingMatcher<T>( expected.getClass() )
        {
            @Override
            protected boolean matchesSafely( T actual, org.hamcrest.Description mismatchDescription )
            {
                U e = mapping.apply( expected );
                U a = mapping.apply( actual );
                if ( Objects.equals( e, a ) )
                {
                    return true;
                }
                mismatchDescription.appendText( "- " );
                mismatchDescription.appendText( "expected: " ).appendValue( e );
                mismatchDescription.appendText( " but was: " ).appendValue( a );
                return false;
            }

            @Override
            public void describeTo( org.hamcrest.Description description )
            {
                description.appendText( trait ).appendText( " should be equal" );
            }
        };
    }
}
