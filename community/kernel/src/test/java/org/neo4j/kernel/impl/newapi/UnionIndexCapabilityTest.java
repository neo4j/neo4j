/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.values.storable.ValueGroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnionIndexCapabilityTest
{
    private static final IndexOrder[] ORDER_CAPABILITIES_ALL = new IndexOrder[]{IndexOrder.ASCENDING, IndexOrder.DESCENDING};
    private static final IndexOrder[] ORDER_CAPABILITIES_ONLY_ASC = new IndexOrder[]{IndexOrder.ASCENDING};
    private static final IndexOrder[] ORDER_CAPABILITIES_ONLY_DES = new IndexOrder[]{IndexOrder.DESCENDING};
    private static final IndexOrder[] ORDER_CAPABILITIES_NONE = new IndexOrder[0];

    @Test
    public void shouldCreateUnionOfOrderCapabilities() throws Exception
    {
        // given
        UnionIndexCapability union;
        union = unionOfOrderCapabilities( ORDER_CAPABILITIES_NONE, ORDER_CAPABILITIES_ONLY_ASC );

        // then
        assertOrderCapability( union, ORDER_CAPABILITIES_ONLY_ASC );

        // given
        union = unionOfOrderCapabilities( ORDER_CAPABILITIES_NONE, ORDER_CAPABILITIES_ALL );

        // then
        assertOrderCapability( union, ORDER_CAPABILITIES_ALL );

        // given
        union = unionOfOrderCapabilities( ORDER_CAPABILITIES_ONLY_ASC, ORDER_CAPABILITIES_ONLY_DES );

        // then
        assertOrderCapability( union, ORDER_CAPABILITIES_ALL );

        // given
        union = unionOfOrderCapabilities( ORDER_CAPABILITIES_ONLY_ASC, ORDER_CAPABILITIES_ALL );

        // then
        assertOrderCapability( union, ORDER_CAPABILITIES_ALL );

        // given
        union = unionOfOrderCapabilities( ORDER_CAPABILITIES_ONLY_ASC, ORDER_CAPABILITIES_ONLY_ASC );

        // then
        assertOrderCapability( union, ORDER_CAPABILITIES_ONLY_ASC );
    }

    @Test
    public void shouldCreateUnionOfValueCapability() throws Exception
    {
        UnionIndexCapability union;

        // given
        union = unionOfValueCapabilities( IndexValueCapability.NO, IndexValueCapability.NO );

        // then
        assertValueCapability( union, IndexValueCapability.NO );

        // given
        union = unionOfValueCapabilities( IndexValueCapability.NO, IndexValueCapability.PARTIAL );

        // then
        assertValueCapability( union, IndexValueCapability.PARTIAL );

        // given
        union = unionOfValueCapabilities( IndexValueCapability.NO, IndexValueCapability.YES );

        // then
        assertValueCapability( union, IndexValueCapability.YES );

        // given
        union = unionOfValueCapabilities( IndexValueCapability.PARTIAL, IndexValueCapability.PARTIAL );

        // then
        assertValueCapability( union, IndexValueCapability.PARTIAL );

        // given
        union = unionOfValueCapabilities( IndexValueCapability.PARTIAL, IndexValueCapability.YES );

        // then
        assertValueCapability( union, IndexValueCapability.YES );

        // given
        union = unionOfValueCapabilities( IndexValueCapability.YES, IndexValueCapability.YES );

        // then
        assertValueCapability( union, IndexValueCapability.YES );
    }

    private UnionIndexCapability unionOfValueCapabilities( IndexValueCapability... valueCapabilities )
    {
        IndexCapability[] capabilities = new IndexCapability[valueCapabilities.length];
        for ( int i = 0; i < valueCapabilities.length; i++ )
        {
            capabilities[i] = capabilityWithValue( valueCapabilities[i] );
        }
        return new UnionIndexCapability( capabilities );
    }

    private UnionIndexCapability unionOfOrderCapabilities( IndexOrder[]... indexOrders )
    {
        IndexCapability[] capabilities = new IndexCapability[indexOrders.length];
        for ( int i = 0; i < indexOrders.length; i++ )
        {
            capabilities[i] = capabilityWithOrder( indexOrders[i] );
        }
        return new UnionIndexCapability( capabilities );
    }

    private IndexCapability capabilityWithValue( IndexValueCapability valueCapability )
    {
        IndexCapability mock = mock( IndexCapability.class );
        when( mock.valueCapability( any() ) ).thenReturn( valueCapability );
        return mock;
    }

    private IndexCapability capabilityWithOrder( IndexOrder[] indexOrder )
    {
        IndexCapability mock = mock( IndexCapability.class );
        when( mock.orderCapability( any() ) ).thenReturn( indexOrder );
        return mock;
    }

    private void assertValueCapability( UnionIndexCapability union, IndexValueCapability expectedValueCapability )
    {
        IndexValueCapability actual = union.valueCapability( someValueGroup() );
        assertEquals( expectedValueCapability, actual );
    }

    private void assertOrderCapability( UnionIndexCapability union, IndexOrder... expected )
    {
        IndexOrder[] actual = union.orderCapability( someValueGroup() );
        assertTrue( "Actual contains all expected", ArrayUtil.containsAll( expected, actual ) );
        assertTrue( "Actual contains nothing else than expected", ArrayUtil.containsAll( actual, expected ) );
    }

    private ValueGroup someValueGroup()
    {
        return ValueGroup.TEXT;
    }
}
