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
package org.neo4j.kernel.impl.api.state;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;
import org.neo4j.values.storable.ValueTuple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.toSet;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Pair.of;

public class TxStateCompositeIndexTest
{
    private TransactionState state;

    private final SchemaIndexDescriptor indexOn_1_1_2 = SchemaIndexDescriptorFactory.forLabel( 1, 1, 2 );
    private final SchemaIndexDescriptor indexOn_1_2_3 = SchemaIndexDescriptorFactory.forLabel( 1, 2, 3 );
    private final SchemaIndexDescriptor indexOn_2_2_3 = SchemaIndexDescriptorFactory.uniqueForLabel( 2, 2, 3 );
    private final SchemaIndexDescriptor indexOn_2_2_3_4 = SchemaIndexDescriptorFactory.forLabel( 2, 2, 3, 4 );

    @Before
    public void before()
    {
        state = new TxState();
    }

    @Test
    public void shouldScanOnAnEmptyTxState()
    {
        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1_2 );

        // THEN
        assertTrue( diffSets.isEmpty() );
    }

    @Test
    public void shouldSeekOnAnEmptyTxState()
    {
        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, ValueTuple.of( "43value1", "43value2" ) );

        // THEN
        assertTrue( diffSets.isEmpty() );
    }

    @Test
    public void shouldScanWhenThereAreNewNodes()
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        modifyIndex( indexOn_1_2_3 ).addDefaultStringEntries( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1_2 );

        // THEN
        assertEquals( asSet( 42L, 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldSeekWhenThereAreNewStringNodes()
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        modifyIndex( indexOn_1_2_3 ).addDefaultStringEntries( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, ValueTuple.of( "43value1", "43value2" ) );

        // THEN
        assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldSeekWhenThereAreNewNumberNodes()
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringProperties( 42L, 43L );
        modifyIndex( indexOn_1_2_3 ).addDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, ValueTuple.of( 43001.0, 43002.0 ) );

        // THEN
        assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldHandleMixedAddsAndRemovesEntry()
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        modifyIndex( indexOn_1_1_2 ).removeDefaultStringEntries( 43L );
        modifyIndex( indexOn_1_1_2 ).removeDefaultStringEntries( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1_2 );

        // THEN
        assertEquals( asSet( 42L ), toSet( diffSets.getAdded() ) );
        assertEquals( asSet( 44L ), toSet( diffSets.getRemoved() ) );
    }

    @Test
    public void shouldSeekWhenThereAreManyEntriesWithTheSameValues()
    {
        // GIVEN
        modifyIndex( indexOn_1_1_2 ).addDefaultStringEntries( 42L, 43L );
        state.indexDoUpdateEntry( indexOn_1_1_2.schema(), 44L, null,
                getDefaultStringPropertyValues( 43L, indexOn_1_1_2.schema().getPropertyIds() ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForSeek( indexOn_1_1_2, ValueTuple.of( "43value1", "43value2" ) );

        // THEN
        assertEquals( asSet( 43L, 44L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldSeekInComplexMix()
    {
        // GIVEN
        ValueTuple[] values21 = Iterators.array(
                ValueTuple.of( "hi", 3 ),
                ValueTuple.of( 9L, 33L ),
                ValueTuple.of( "sneaker", false ) );

        ValueTuple[] values22 = Iterators.array(
                ValueTuple.of( true, false ),
                ValueTuple.of( new int[]{ 10,100}, "array-buddy" ),
                ValueTuple.of( 40.1, 40.2 ) );

        ValueTuple[] values3 = Iterators.array(
                ValueTuple.of( "hi", "ho", "hello" ),
                ValueTuple.of( true, new long[]{4L}, 33L ),
                ValueTuple.of( 2, false, 1 ) );

        addEntries( indexOn_1_1_2, values21, 10 );
        addEntries( indexOn_2_2_3, values22, 100 );
        addEntries( indexOn_2_2_3_4, values3, 1000 );

        assertSeek( indexOn_1_1_2, values21, 10 );
        assertSeek( indexOn_2_2_3, values22, 100 );
        assertSeek( indexOn_2_2_3_4, values3, 1000 );
    }

    private void addEntries( SchemaIndexDescriptor index, ValueTuple[] values, long nodeIdStart )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            state.indexDoUpdateEntry( index.schema(), nodeIdStart + i, null, values[i] );
        }
    }

    private void assertSeek( SchemaIndexDescriptor index, ValueTuple[] values, long nodeIdStart )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            // WHEN
            PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForSeek( index, values[i] );

            // THEN
            assertEquals( asSet( nodeIdStart + i ), toSet( diffSets.getAdded() ) );
        }
    }

    private interface IndexUpdater
    {
        void addDefaultStringEntries( long... nodeIds );

        void removeDefaultStringEntries( long... nodeIds );

        void addDefaultStringProperties( long... nodeIds );
    }

    private IndexUpdater modifyIndex( final SchemaIndexDescriptor descriptor )
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
                Collection<Pair<Long,ValueTuple>> entries = new ArrayList<>( nodeIds.length );
                for ( long nodeId : nodeIds )
                {
                    int[] propertyIds = descriptor.schema().getPropertyIds();
                    Object[] values = new Object[propertyIds.length];
                    for ( int i = 0; i < propertyIds.length; i++ )
                    {
                        values[i] = nodeId * 1000.0 + propertyIds[i];
                    }
                    entries.add( of( nodeId, ValueTuple.of( values ) ) );
                }
                addEntries( entries );
            }

            void addEntries( Collection<Pair<Long,ValueTuple>> nodesWithValues )
            {
                for ( Pair<Long,ValueTuple> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.indexDoUpdateEntry( descriptor.schema(), nodeId, null, entry.other() );
                }
            }

            void removeEntries( Collection<Pair<Long,ValueTuple>> nodesWithValues )
            {
                for ( Pair<Long,ValueTuple> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.indexDoUpdateEntry( descriptor.schema(), nodeId, entry.other(), null );
                }
            }

            private Collection<Pair<Long,ValueTuple>> getDefaultStringEntries( long[] nodeIds )
            {
                Collection<Pair<Long,ValueTuple>> entries = new ArrayList<>( nodeIds.length );
                for ( long nodeId : nodeIds )
                {
                    int[] propertyIds = descriptor.schema().getPropertyIds();
                    ValueTuple values = getDefaultStringPropertyValues( nodeId, propertyIds );
                    entries.add( of( nodeId, values ) );
                }
                return entries;
            }
        };
    }

    private ValueTuple getDefaultStringPropertyValues( long nodeId, int[] propertyIds )
    {
        Object[] values = new Object[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            values[i] = nodeId + "value" + propertyIds[i];
        }
        return ValueTuple.of( values );
    }
}
