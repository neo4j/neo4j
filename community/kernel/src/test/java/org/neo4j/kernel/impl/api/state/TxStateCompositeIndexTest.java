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
package org.neo4j.kernel.impl.api.state;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.schema.OrderedPropertyValues;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Pair.of;

public class TxStateCompositeIndexTest
{
    private TransactionState state;

    private final IndexDescriptor indexOn_1_1_2 = IndexDescriptorFactory.forLabel( 1, 1, 2 );
    private final IndexDescriptor indexOn_1_2_3 = IndexDescriptorFactory.forLabel( 1, 2, 3 );
    private final IndexDescriptor indexOn_2_2_3 = IndexDescriptorFactory.uniqueForLabel( 2, 2, 3 );
    private final IndexDescriptor indexOn_2_2_3_4 = IndexDescriptorFactory.forLabel( 2, 2, 3, 4 );

    @Before
    public void before() throws Exception
    {
        state = new TxState();
    }

    @Test
    public void shouldScanOnAnEmptyTxState() throws Exception
    {
        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForScan( indexOn_1_1_2 );

        // THEN
        assertTrue( diffSets.isEmpty() );
    }

    @Test
    public void shouldSeekOnAnEmptyTxState() throws Exception
    {
        // WHEN
        ReadableDiffSets<Long> diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, OrderedPropertyValues.ofUndefined( "43value1", "43value2" ) );

        // THEN
        assertTrue( diffSets.isEmpty() );
    }

    @Test
    public void shouldScanWhenThereAreNewNodes() throws Exception
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        modifyIndex( indexOn_1_2_3 ).addDefaultStringEntries( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForScan( indexOn_1_1_2 );

        // THEN
        assertEquals( asSet( 42L, 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldSeekWhenThereAreNewStringNodes() throws Exception
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        modifyIndex( indexOn_1_2_3 ).addDefaultStringEntries( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, OrderedPropertyValues.ofUndefined( "43value1", "43value2" ) );

        // THEN
        assertEquals( asSet( 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldSeekWhenThereAreNewNumberNodes() throws Exception
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringProperties( 42L, 43L );
        modifyIndex( indexOn_1_2_3 ).addDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, OrderedPropertyValues.ofUndefined( 43001.0, 43002.0 ) );

        // THEN
        assertEquals( asSet( 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldHandleMixedAddsAndRemovesEntry() throws Exception
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        modifyIndex( indexOn_1_1_2 ).removeDefaultStringEntries( 43L );
        modifyIndex( indexOn_1_1_2 ).removeDefaultStringEntries( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets =
                state.indexUpdatesForScan( indexOn_1_1_2 );

        // THEN
        assertEquals( asSet( 42L ), diffSets.getAdded() );
        assertEquals( asSet( 44L ), diffSets.getRemoved() );
    }

    @Test
    public void shouldSeekWhenThereAreManyEntriesWithTheSameValues() throws Exception
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        state.indexDoUpdateEntry( indexOn_1_1_2.schema(), 44L, null,
                getDefaultStringPropertyValues( 43L, indexOn_1_1_2.schema().getPropertyIds() ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, OrderedPropertyValues.ofUndefined( "43value1", "43value2" ) );

        // THEN
        assertEquals( asSet( 43L, 44L ), diffSets.getAdded() );
    }

    @Test
    public void shouldSeekInComplexMix() throws Exception
    {
        // GIVEN
        OrderedPropertyValues[] values2_1 = Iterators.array(
                OrderedPropertyValues.ofUndefined( "hi", 3 ),
                OrderedPropertyValues.ofUndefined( 9L, 33L ),
                OrderedPropertyValues.ofUndefined( "sneaker", false ) );

        OrderedPropertyValues[] values2_2 = Iterators.array(
                OrderedPropertyValues.ofUndefined( true, false ),
                OrderedPropertyValues.ofUndefined( new int[]{ 10,100}, "array-buddy" ),
                OrderedPropertyValues.ofUndefined( 40.1, 40.2 ) );

        OrderedPropertyValues[] values3 = Iterators.array(
                OrderedPropertyValues.ofUndefined( "hi", "ho", "hello" ),
                OrderedPropertyValues.ofUndefined( true, new long[]{4L}, 33L ),
                OrderedPropertyValues.ofUndefined( 2, false, 1 ) );

        addEntries( indexOn_1_1_2, values2_1, 10 );
        addEntries( indexOn_2_2_3, values2_2, 100 );
        addEntries( indexOn_2_2_3_4, values3, 1000 );

        assertSeek( indexOn_1_1_2, values2_1, 10 );
        assertSeek( indexOn_2_2_3, values2_2, 100 );
        assertSeek( indexOn_2_2_3_4, values3, 1000 );
    }

    private void addEntries( IndexDescriptor index, OrderedPropertyValues[] values, long nodeIdStart )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            state.indexDoUpdateEntry( index.schema(), nodeIdStart + i, null, values[i] );
        }
    }

    private void assertSeek( IndexDescriptor index, OrderedPropertyValues[] values, long nodeIdStart )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            // WHEN
            ReadableDiffSets<Long> diffSets = state.indexUpdatesForSeek( index, values[i] );

            // THEN
            assertEquals( asSet( nodeIdStart + i ), diffSets.getAdded() );
        }
    }

    private interface IndexUpdater
    {
        void addDefaultStringEntries( long... nodeIds );

        void removeDefaultStringEntries( long... nodeIds );

        void addDefaultStringProperties( long... nodeIds );
    }

    private IndexUpdater modifyIndex( final IndexDescriptor descriptor )
    {
        return new IndexUpdater()
        {
            @Override
            public void addDefaultStringEntries( long... nodeIds )
            {
                addEntries( getDefaultStringEntries( nodeIds ) );
            }

            @Override
            public void removeDefaultStringEntries( long... nodeIds )
            {
                removeEntries( getDefaultStringEntries( nodeIds ) );
            }

            @Override
            public void addDefaultStringProperties( long... nodeIds )
            {
                Collection<Pair<Long,OrderedPropertyValues>> entries = new ArrayList<>( nodeIds.length );
                for ( long nodeId : nodeIds )
                {
                    int[] propertyIds = descriptor.schema().getPropertyIds();
                    Object[] values = new Object[propertyIds.length];
                    for ( int i = 0; i < propertyIds.length; i++ )
                    {
                        values[i] = nodeId * 1000.0 + propertyIds[i];
                    }
                    entries.add( of( nodeId, OrderedPropertyValues.ofUndefined( values ) ) );
                }
                addEntries( entries );
            }

            void addEntries( Collection<Pair<Long,OrderedPropertyValues>> nodesWithValues )
            {
                for ( Pair<Long,OrderedPropertyValues> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.indexDoUpdateEntry( descriptor.schema(), nodeId, null, entry.other() );
                }
            }

            void removeEntries( Collection<Pair<Long,OrderedPropertyValues>> nodesWithValues )
            {
                for ( Pair<Long,OrderedPropertyValues> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.indexDoUpdateEntry( descriptor.schema(), nodeId, entry.other(), null );
                }
            }

            private Collection<Pair<Long,OrderedPropertyValues>> getDefaultStringEntries( long[] nodeIds )
            {
                Collection<Pair<Long,OrderedPropertyValues>> entries = new ArrayList<>( nodeIds.length );
                for ( long nodeId : nodeIds )
                {
                    int[] propertyIds = descriptor.schema().getPropertyIds();
                    OrderedPropertyValues values = getDefaultStringPropertyValues( nodeId, propertyIds );
                    entries.add( of( nodeId, values ) );
                }
                return entries;
            }
        };
    }

    private OrderedPropertyValues getDefaultStringPropertyValues( long nodeId, int[] propertyIds )
    {
        Object[] values = new Object[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            values[i] = nodeId + "value" + propertyIds[i];
        }
        return OrderedPropertyValues.ofUndefined( values );
    }
}
