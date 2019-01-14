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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;

public class IndexUpdaterMapTest
{
    private IndexMap indexMap;

    private IndexProxy indexProxy1;
    private CapableIndexDescriptor schemaIndexDescriptor1;
    private IndexUpdater indexUpdater1;

    private IndexProxy indexProxy2;
    private CapableIndexDescriptor schemaIndexDescriptor2;

    private IndexProxy indexProxy3;
    private CapableIndexDescriptor schemaIndexDescriptor3;

    private IndexUpdaterMap updaterMap;

    @Before
    public void before()
    {
        indexMap = new IndexMap();

        indexProxy1 = mock( IndexProxy.class );
        schemaIndexDescriptor1 = forSchema( forLabel( 2, 3 ), PROVIDER_DESCRIPTOR ).withId( 0 ).withoutCapabilities();
        indexUpdater1 = mock( IndexUpdater.class );
        when( indexProxy1.getDescriptor() ).thenReturn( schemaIndexDescriptor1 );
        when( indexProxy1.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( indexUpdater1 );

        indexProxy2 = mock( IndexProxy.class );
        schemaIndexDescriptor2 = forSchema( forLabel( 5, 6 ), PROVIDER_DESCRIPTOR ).withId( 1 ).withoutCapabilities();
        IndexUpdater indexUpdater2 = mock( IndexUpdater.class );
        when( indexProxy2.getDescriptor() ).thenReturn( schemaIndexDescriptor2 );
        when( indexProxy2.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( indexUpdater2 );

        indexProxy3 = mock( IndexProxy.class );
        schemaIndexDescriptor3 = forSchema( forLabel( 5, 7, 8 ), PROVIDER_DESCRIPTOR ).withId( 2 ).withoutCapabilities();
        IndexUpdater indexUpdater3 = mock( IndexUpdater.class );
        when( indexProxy3.getDescriptor() ).thenReturn( schemaIndexDescriptor3 );
        when( indexProxy3.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( indexUpdater3 );

        updaterMap = new IndexUpdaterMap( indexMap, IndexUpdateMode.ONLINE );
    }

    @Test
    public void shouldRetrieveUpdaterFromIndexMapForExistingIndex()
    {
        // given
        indexMap.putIndexProxy( indexProxy1 );

        // when
        IndexUpdater updater = updaterMap.getUpdater( schemaIndexDescriptor1.schema() );

        // then
        assertEquals( indexUpdater1, updater );
        assertEquals( 1, updaterMap.size() );
    }

    @Test
    public void shouldRetrieveUpdateUsingLabelAndProperty()
    {
        // given
        indexMap.putIndexProxy( indexProxy1 );

        // when
        IndexUpdater updater = updaterMap.getUpdater( schemaIndexDescriptor1.schema() );

        // then
        assertThat( updater, equalTo( indexUpdater1 ) );
    }

    @Test
    public void shouldRetrieveSameUpdaterFromIndexMapForExistingIndexWhenCalledTwice()
    {
        // given
        indexMap.putIndexProxy( indexProxy1 );

        // when
        IndexUpdater updater1 = updaterMap.getUpdater( schemaIndexDescriptor1.schema() );
        IndexUpdater updater2 = updaterMap.getUpdater( schemaIndexDescriptor1.schema() );

        // then
        assertEquals( updater1, updater2 );
        assertEquals( 1, updaterMap.size() );
    }

    @Test
    public void shouldRetrieveNoUpdaterForNonExistingIndex()
    {
        // when
        IndexUpdater updater = updaterMap.getUpdater( schemaIndexDescriptor1.schema() );

        // then
        assertNull( updater );
        assertTrue( "updater map must be empty", updaterMap.isEmpty() );
    }

    @Test
    public void shouldCloseAllUpdaters() throws Exception
    {
        // given
        indexMap.putIndexProxy( indexProxy1 );
        indexMap.putIndexProxy( indexProxy2 );

        IndexUpdater updater1 = updaterMap.getUpdater( schemaIndexDescriptor1.schema() );
        IndexUpdater updater2 = updaterMap.getUpdater( schemaIndexDescriptor2.schema() );

        // hen
        updaterMap.close();

        // then
        verify( updater1 ).close();
        verify( updater2 ).close();

        assertTrue( "updater map must be empty", updaterMap.isEmpty() );
    }
}
