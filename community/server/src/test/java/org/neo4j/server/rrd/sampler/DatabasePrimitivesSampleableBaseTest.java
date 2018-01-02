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
package org.neo4j.server.rrd.sampler;

import org.junit.Test;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabasePrimitivesSampleableBaseTest
{
    @Test
    public void shouldSampleOnlyWhenDatabaseIsAvailable() throws Throwable
    {
        // given
        double expected = 42d;
        TheDatabasePrimitivesSampleableBase sampleable = createSampleable( true, expected );

        // when
        double value = sampleable.getValue();

        // then
        assertEquals( expected, value, 0d );
    }

    @Test
    public void shouldNotSampleWhenDatabaseIsNotAvailable() throws Throwable
    {
        // given
        TheDatabasePrimitivesSampleableBase sampleable = createSampleable( false, 42d );

        // when
        double value = sampleable.getValue();

        // then
        assertEquals( 0d, value, 0d );
    }

    private TheDatabasePrimitivesSampleableBase createSampleable( final boolean isAvailable, double sampleValue )
    {
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        when( guard.isAvailable( 0 ) ).thenReturn( isAvailable );
        return new TheDatabasePrimitivesSampleableBase( sampleValue, guard );
    }

    private static class TheDatabasePrimitivesSampleableBase extends DatabasePrimitivesSampleableBase
    {
        private final double sampleValue;

        public TheDatabasePrimitivesSampleableBase( double sampleValue, AvailabilityGuard guard )
        {
            super( mock( NeoStoresSupplier.class ), guard );
            this.sampleValue = sampleValue;
        }

        @Override
        protected double readValue( NeoStores neoStore )
        {
            return sampleValue;
        }

        @Override
        public String getName()
        {
            return "name";
        }
    }
}
