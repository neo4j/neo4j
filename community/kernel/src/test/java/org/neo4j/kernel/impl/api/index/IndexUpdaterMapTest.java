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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexUpdaterMapTest
{
    private IndexMap indexMap;

    private IndexProxy indexProxy1;
    private NewIndexDescriptor indexDescriptor1;
    private IndexUpdater indexUpdater1;

    private IndexProxy indexProxy2;
    private NewIndexDescriptor indexDescriptor2;

    private IndexProxy indexProxy3;
    private NewIndexDescriptor indexDescriptor3;

    private IndexUpdaterMap updaterMap;

    @Before
    public void before() throws IOException
    {
        indexMap = new IndexMap();

        indexProxy1 = mock( IndexProxy.class );
        indexDescriptor1 = NewIndexDescriptorFactory.forLabel( 2, 3 );
        indexUpdater1 = mock( IndexUpdater.class );
        when( indexProxy1.getDescriptor() ).thenReturn( indexDescriptor1 );
        when( indexProxy1.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( indexUpdater1 );

        indexProxy2 = mock( IndexProxy.class );
        indexDescriptor2 = NewIndexDescriptorFactory.forLabel( 5, 6 );
        IndexUpdater indexUpdater2 = mock( IndexUpdater.class );
        when( indexProxy2.getDescriptor() ).thenReturn( indexDescriptor2 );
        when( indexProxy2.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( indexUpdater2 );

        indexProxy3 = mock( IndexProxy.class );
        indexDescriptor3 = NewIndexDescriptorFactory.forLabel( 5, 7, 8 );
        IndexUpdater indexUpdater3 = mock( IndexUpdater.class );
        when( indexProxy3.getDescriptor() ).thenReturn( indexDescriptor3 );
        when( indexProxy3.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( indexUpdater3 );

        updaterMap = new IndexUpdaterMap( indexMap, IndexUpdateMode.ONLINE );
    }

    @Test
    public void shouldRetrieveUpdaterFromIndexMapForExistingIndex() throws Exception
    {
        // given
        indexMap.putIndexProxy( 0, indexProxy1 );

        // when
        IndexUpdater updater = updaterMap.getUpdater( indexDescriptor1.schema() );

        // then
        assertEquals( indexUpdater1, updater );
        assertEquals( 1, updaterMap.size() );
    }

    @Test
    public void shouldRetrieveUpdateUsingLabelAndProperty()
    {
        // given
        indexMap.putIndexProxy( 0, indexProxy1 );

        // when
        IndexUpdater updater = updaterMap.getUpdater( indexDescriptor1.schema() );

        // then
        assertThat( updater, equalTo( indexUpdater1 ) );
    }

    @Test
    public void shouldRetrieveSameUpdaterFromIndexMapForExistingIndexWhenCalledTwice() throws Exception
    {
        // given
        indexMap.putIndexProxy( 0, indexProxy1 );

        // when
        IndexUpdater updater1 = updaterMap.getUpdater( indexDescriptor1.schema() );
        IndexUpdater updater2 = updaterMap.getUpdater( indexDescriptor1.schema() );

        // then
        assertEquals( updater1, updater2 );
        assertEquals( 1, updaterMap.size() );
    }

    @Test
    public void shouldRetrieveNoUpdaterForNonExistingIndex() throws Exception
    {
        // when
        IndexUpdater updater = updaterMap.getUpdater( indexDescriptor1.schema() );

        // then
        assertNull( updater );
        assertTrue( "updater map must be empty", updaterMap.isEmpty() );
    }

    @Test
    public void shouldCloseAllUpdaters() throws Exception
    {
        // given
        indexMap.putIndexProxy( 0, indexProxy1 );
        indexMap.putIndexProxy( 1, indexProxy2 );

        IndexUpdater updater1 = updaterMap.getUpdater( indexDescriptor1.schema() );
        IndexUpdater updater2 = updaterMap.getUpdater( indexDescriptor2.schema() );

        // hen
        updaterMap.close();

        // then
        verify( updater1 ).close();
        verify( updater2 ).close();

        assertTrue( "updater map must be empty", updaterMap.isEmpty() );
    }

    @Test
    public void shouldGetUpdatersForLabel()
    {
        // given
        indexMap.putIndexProxy( 0, indexProxy1 );
        indexMap.putIndexProxy( 1, indexProxy2 );

        // when
        List<IndexUpdaterMap.IndexUpdaterWithSchema> updaters = Iterables.asList(
                updaterMap.updatersForLabels(
                        new long[]{
                                indexDescriptor1.schema().getLabelId()
                        } ) );

        // then
        assertThat( updaters, hasSize( 1 ) );
        assertThat( updaters.get( 0 ).schema(), equalTo( indexDescriptor1.schema() ) );
    }

    @Test
    public void shouldGetUpdatersForLabels()
    {
        // given
        indexMap.putIndexProxy( 0, indexProxy1 );
        indexMap.putIndexProxy( 1, indexProxy2 );

        // when
        List<IndexUpdaterMap.IndexUpdaterWithSchema> updaters = Iterables.asList(
                updaterMap.updatersForLabels(
                        new long[]{
                                indexDescriptor1.schema().getLabelId(),
                                indexDescriptor2.schema().getLabelId()
                        } ) );

        // then
        assertThat( updaters, hasSize( 2 ) );
        assertThat( updaters.get( 0 ).schema(), equalTo( indexDescriptor1.schema() ) );
        assertThat( updaters.get( 1 ).schema(), equalTo( indexDescriptor2.schema() ) );
    }

    @Test
    public void shouldGetMultipleUpdatersForLabels()
    {
        // given
        indexMap.putIndexProxy( 0, indexProxy1 );
        indexMap.putIndexProxy( 1, indexProxy2 );
        indexMap.putIndexProxy( 2, indexProxy3 );

        // when
        int labelIdFor2and3 = indexDescriptor2.schema().getLabelId();

        Set<LabelSchemaDescriptor> updaters =
                Iterables.stream(
                    updaterMap.updatersForLabels(
                        new long[]{labelIdFor2and3} ) )
                .map( IndexUpdaterMap.IndexUpdaterWithSchema::schema )
                .collect( Collectors.toSet() );

        // then
        assertThat( updaters, contains( indexDescriptor2.schema(), indexDescriptor3.schema() ) );
    }
}
