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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.array;

@ExtendWith( RandomExtension.class )
class BaseRecordFormatsTest
{
    private static final RecordStorageCapability[] CAPABILITIES = RecordStorageCapability.values();
    private static final CapabilityType[] CAPABILITY_TYPES = CapabilityType.values();

    @Inject
    private RandomRule random;

    @Test
    void shouldReportCompatibilityBetweenTwoEqualSetsOfCapabilities()
    {
        // given
        RecordStorageCapability[] capabilities = random.selection( CAPABILITIES, CAPABILITIES.length / 2, CAPABILITIES.length, false );

        // then
        assertCompatibility( capabilities, capabilities, true, CAPABILITY_TYPES );
    }

    @Test
    void shouldReportCompatibilityForAdditiveAdditionalCapabilities()
    {
        // given
        RecordStorageCapability[] from = array( RecordStorageCapability.SCHEMA );
        RecordStorageCapability[] to = array( RecordStorageCapability.SCHEMA, RecordStorageCapability.POINT_PROPERTIES,
            RecordStorageCapability.TEMPORAL_PROPERTIES );

        // then
        assertCompatibility( from, to, true, CAPABILITY_TYPES );
    }

    @Test
    void shouldReportIncompatibilityForChangingAdditionalCapabilities()
    {
        // given
        RecordStorageCapability[] from = array( RecordStorageCapability.SCHEMA );
        RecordStorageCapability[] to = array( RecordStorageCapability.SCHEMA, RecordStorageCapability.DENSE_NODES );

        // then
        assertCompatibility( from, to, false, CapabilityType.STORE );
    }

    @Test
    void shouldReportIncompatibilityForAdditiveRemovedCapabilities()
    {
        // given
        RecordStorageCapability[] from = array( RecordStorageCapability.SCHEMA, RecordStorageCapability.POINT_PROPERTIES,
                RecordStorageCapability.TEMPORAL_PROPERTIES );
        RecordStorageCapability[] to = array( RecordStorageCapability.SCHEMA );

        // then
        assertCompatibility( from, to, false, CapabilityType.STORE );
    }

    private void assertCompatibility( RecordStorageCapability[] from, RecordStorageCapability[] to, boolean compatible, CapabilityType... capabilityTypes )
    {
        for ( CapabilityType type : capabilityTypes )
        {
            assertEquals( compatible, format( from ).hasCompatibleCapabilities( format( to ), type ) );
        }
    }

    private RecordFormats format( RecordStorageCapability... capabilities )
    {
        RecordFormats formats = mock( BaseRecordFormats.class );
        when( formats.capabilities() ).thenReturn( capabilities );
        when( formats.hasCompatibleCapabilities( any( RecordFormats.class ), any( CapabilityType.class ) ) ).thenCallRealMethod();
        return formats;
    }
}
