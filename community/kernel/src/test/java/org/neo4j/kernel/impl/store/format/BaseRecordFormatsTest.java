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
package org.neo4j.kernel.impl.store.format;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.array;

public class BaseRecordFormatsTest
{
    private static final Capability[] CAPABILITIES = Capability.values();
    private static final CapabilityType[] CAPABILITY_TYPES = CapabilityType.values();

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldReportCompatibilityBetweenTwoEqualSetsOfCapabilities()
    {
        // given
        Capability[] capabilities = random.selection( CAPABILITIES, CAPABILITIES.length / 2, CAPABILITIES.length, false );

        // then
        assertCompatibility( capabilities, capabilities, true, CAPABILITY_TYPES );
    }

    @Test
    public void shouldReportCompatibilityForAdditiveAdditionalCapabilities()
    {
        // given
        Capability[] from = array( Capability.SCHEMA );
        Capability[] to = array( Capability.SCHEMA, Capability.POINT_PROPERTIES, Capability.TEMPORAL_PROPERTIES );

        // then
        assertCompatibility( from, to, true, CAPABILITY_TYPES );
    }

    @Test
    public void shouldReportIncompatibilityForChangingAdditionalCapabilities()
    {
        // given
        Capability[] from = array( Capability.SCHEMA );
        Capability[] to = array( Capability.SCHEMA, Capability.DENSE_NODES );

        // then
        assertCompatibility( from, to, false, CapabilityType.STORE );
    }

    @Test
    public void shouldReportIncompatibilityForAdditiveRemovedCapabilities()
    {
        // given
        Capability[] from = array( Capability.SCHEMA, Capability.POINT_PROPERTIES, Capability.TEMPORAL_PROPERTIES );
        Capability[] to = array( Capability.SCHEMA );

        // then
        assertCompatibility( from, to, false, CapabilityType.STORE );
    }

    private void assertCompatibility( Capability[] from, Capability[] to, boolean compatible, CapabilityType... capabilityTypes )
    {
        for ( CapabilityType type : capabilityTypes )
        {
            assertEquals( compatible, format( from ).hasCompatibleCapabilities( format( to ), type ) );
        }
    }

    private RecordFormats format( Capability... capabilities )
    {
        RecordFormats formats = mock( BaseRecordFormats.class );
        when( formats.capabilities() ).thenReturn( capabilities );
        when( formats.hasCompatibleCapabilities( any( RecordFormats.class ), any( CapabilityType.class ) ) ).thenCallRealMethod();
        return formats;
    }
}
