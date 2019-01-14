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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;

import java.util.Collections;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexLimitation;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.values.storable.ValueCategory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class UnionIndexCapabilityTest
{
    private static final IndexOrder[] ORDER_CAPABILITIES_ALL = new IndexOrder[]{IndexOrder.ASCENDING, IndexOrder.DESCENDING};
    private static final IndexOrder[] ORDER_CAPABILITIES_ONLY_ASC = new IndexOrder[]{IndexOrder.ASCENDING};
    private static final IndexOrder[] ORDER_CAPABILITIES_ONLY_DES = new IndexOrder[]{IndexOrder.DESCENDING};
    private static final IndexOrder[] ORDER_CAPABILITIES_NONE = new IndexOrder[0];

    @Test
    public void shouldCreateUnionOfOrderCapabilities()
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
    public void shouldCreateUnionOfValueCapability()
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

    @Test
    public void shouldCreateUnionOfIndexLimitations()
    {
        UnionIndexCapability union;

        // given
        union = unionOfIndexLimitations( IndexCapability.LIMITIATION_NONE, IndexCapability.LIMITIATION_NONE );

        // then
        assertEquals( Collections.emptySet(), asSet( union.limitations() ) );

        // given
        union = unionOfIndexLimitations( IndexCapability.LIMITIATION_NONE, array( IndexLimitation.SLOW_CONTAINS ) );

        // then
        assertEquals( asSet( IndexLimitation.SLOW_CONTAINS ), asSet( union.limitations() ) );

        // given
        union = unionOfIndexLimitations( array( IndexLimitation.SLOW_CONTAINS ), array( IndexLimitation.SLOW_CONTAINS ) );

        // then
        assertEquals( asSet( IndexLimitation.SLOW_CONTAINS ), asSet( union.limitations() ) );
    }

    private UnionIndexCapability unionOfIndexLimitations( IndexLimitation[]... limitiations )
    {
        IndexCapability[] capabilities = new IndexCapability[limitiations.length];
        for ( int i = 0; i < limitiations.length; i++ )
        {
            capabilities[i] = capabilityWithIndexLimitations( limitiations[i] );
        }
        return new UnionIndexCapability( capabilities );
    }

    private IndexCapability capabilityWithIndexLimitations( IndexLimitation[] limitations )
    {
        IndexCapability mock = mockedIndexCapability();
        when( mock.limitations() ).thenReturn( limitations );
        return mock;
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
        IndexCapability mock = mockedIndexCapability();
        when( mock.valueCapability( any() ) ).thenReturn( valueCapability );
        return mock;
    }

    private IndexCapability mockedIndexCapability()
    {
        IndexCapability mock = mock( IndexCapability.class );
        when( mock.limitations() ).thenReturn( IndexCapability.LIMITIATION_NONE );
        return mock;
    }

    private IndexCapability capabilityWithOrder( IndexOrder[] indexOrder )
    {
        IndexCapability mock = mockedIndexCapability();
        when( mock.orderCapability( any() ) ).thenReturn( indexOrder );
        return mock;
    }

    private void assertValueCapability( UnionIndexCapability union, IndexValueCapability expectedValueCapability )
    {
        IndexValueCapability actual = union.valueCapability( someValueCategory() );
        assertEquals( expectedValueCapability, actual );
    }

    private void assertOrderCapability( UnionIndexCapability union, IndexOrder... expected )
    {
        IndexOrder[] actual = union.orderCapability( someValueCategory() );
        assertTrue( "Actual contains all expected", ArrayUtil.containsAll( expected, actual ) );
        assertTrue( "Actual contains nothing else than expected", ArrayUtil.containsAll( actual, expected ) );
    }

    private ValueCategory someValueCategory()
    {
        return ValueCategory.TEXT;
    }
}
