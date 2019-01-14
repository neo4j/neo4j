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
package org.neo4j.unsafe.impl.batchimport;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.Loaders;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.transaction.state.PropertyTraverser;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.NeoStoresRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.RepeatRule.Repeat;
import org.neo4j.unsafe.batchinsert.internal.DirectRecordAccess;
import org.neo4j.unsafe.impl.batchimport.staging.SimpleStageControl;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;

public class DeleteDuplicateNodesStepTest
{
    private final RandomRule random = new RandomRule().withConfiguration( new Randoms.Default()
    {
        @Override
        public int stringMaxLength()
        {
            return 200;
        }

        @Override
        public int arrayMaxLength()
        {
            return 200;
        }
    } );
    private final NeoStoresRule neoStoresRule = new NeoStoresRule( getClass() );
    private final RepeatRule repeater = new RepeatRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( repeater ).around( random ).around( neoStoresRule );

    @Repeat( times = 10 )
    @Test
    public void shouldDeleteEverythingAboutTheDuplicatedNodes() throws Exception
    {
        // given
        NeoStores neoStores = neoStoresRule.builder().build();
        Ids[] ids = new Ids[9];
        DataImporter.Monitor monitor = new DataImporter.Monitor();
        ids[0] = createNode( monitor, neoStores, 10, 10 ); // node with many properties and many labels
        ids[1] = createNode( monitor, neoStores, 10, 1 ); // node with many properties and few labels
        ids[2] = createNode( monitor, neoStores, 10, 0 ); // node with many properties and no labels
        ids[3] = createNode( monitor, neoStores, 1, 10 ); // node with few properties and many labels
        ids[4] = createNode( monitor, neoStores, 1, 1 ); // node with few properties and few labels
        ids[5] = createNode( monitor, neoStores, 1, 0 ); // node with few properties and no labels
        ids[6] = createNode( monitor, neoStores, 0, 10 ); // node with no properties and many labels
        ids[7] = createNode( monitor, neoStores, 0, 1 ); // node with no properties and few labels
        ids[8] = createNode( monitor, neoStores, 0, 0 ); // node with no properties and no labels

        // when
        long[] duplicateNodeIds = randomNodes( ids );
        SimpleStageControl control = new SimpleStageControl();
        try ( DeleteDuplicateNodesStep step = new DeleteDuplicateNodesStep( control, Configuration.DEFAULT,
                PrimitiveLongCollections.iterator( duplicateNodeIds ), neoStores.getNodeStore(), neoStores.getPropertyStore(), monitor ) )
        {
            control.steps( step );
            startAndAwaitCompletionOf( step );
        }
        control.assertHealthy();

        // then
        int expectedNodes = 0;
        int expectedProperties = 0;
        for ( Ids entity : ids )
        {
            boolean expectedToBeInUse = !ArrayUtils.contains( duplicateNodeIds, entity.node.getId() );
            int stride = expectedToBeInUse ? 1 : 0;
            expectedNodes += stride;

            // Verify node record
            assertEquals( expectedToBeInUse, neoStores.getNodeStore().isInUse( entity.node.getId() ) );

            // Verify label records
            for ( DynamicRecord labelRecord : entity.node.getDynamicLabelRecords() )
            {
                assertEquals( expectedToBeInUse, neoStores.getNodeStore().getDynamicLabelStore().isInUse( labelRecord.getId() ) );
            }

            // Verify property records
            for ( PropertyRecord propertyRecord : entity.properties )
            {
                assertEquals( expectedToBeInUse, neoStores.getPropertyStore().isInUse( propertyRecord.getId() ) );
                for ( PropertyBlock property : propertyRecord )
                {
                    // Verify property dynamic value records
                    for ( DynamicRecord valueRecord : property.getValueRecords() )
                    {
                        AbstractDynamicStore valueStore;
                        switch ( property.getType() )
                        {
                        case STRING:
                            valueStore = neoStores.getPropertyStore().getStringStore();
                            break;
                        case ARRAY:
                            valueStore = neoStores.getPropertyStore().getArrayStore();
                            break;
                        default: throw new IllegalArgumentException( propertyRecord + " " + property );
                        }
                        assertEquals( expectedToBeInUse, valueStore.isInUse( valueRecord.getId() ) );
                    }
                    expectedProperties += stride;
                }
            }
        }

        assertEquals( expectedNodes, monitor.nodesImported() );
        assertEquals( expectedProperties, monitor.propertiesImported() );
    }

    private long[] randomNodes( Ids[] ids )
    {
        long[] nodeIds = new long[ids.length];
        int cursor = 0;
        for ( Ids id : ids )
        {
            if ( random.nextBoolean() )
            {
                nodeIds[cursor++] = id.node.getId();
            }
        }

        // If none was selected, then pick just one
        if ( cursor == 0 )
        {
            nodeIds[cursor++] = random.among( ids ).node.getId();
        }
        return Arrays.copyOf( nodeIds, cursor );
    }

    private static class Ids
    {
        private final NodeRecord node;
        private final PropertyRecord[] properties;

        Ids( NodeRecord node, PropertyRecord[] properties )
        {
            this.node = node;
            this.properties = properties;
        }
    }

    private Ids createNode( DataImporter.Monitor monitor, NeoStores neoStores, int propertyCount, int labelCount )
    {
        PropertyStore propertyStore = neoStores.getPropertyStore();
        RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecordAccess =
                new DirectRecordAccess<>( propertyStore, new Loaders( neoStores ).propertyLoader() );
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord nodeRecord = nodeStore.newRecord();
        nodeRecord.setId( nodeStore.nextId() );
        nodeRecord.setInUse( true );
        NodeLabelsField.parseLabelsField( nodeRecord ).put( labelIds( labelCount ), nodeStore, nodeStore.getDynamicLabelStore() );
        long nextProp = new PropertyCreator( propertyStore, new PropertyTraverser() )
                .createPropertyChain( nodeRecord, properties( propertyStore, propertyCount ), propertyRecordAccess );
        nodeRecord.setNextProp( nextProp );
        nodeStore.updateRecord( nodeRecord );
        PropertyRecord[] propertyRecords = extractPropertyRecords( propertyRecordAccess, nextProp );
        propertyRecordAccess.close();
        monitor.nodesImported( 1 );
        monitor.propertiesImported( propertyCount );
        return new Ids( nodeRecord, propertyRecords );
    }

    private static PropertyRecord[] extractPropertyRecords( RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecordAccess,
            long nextProp )
    {
        List<PropertyRecord> result = new ArrayList<>();
        while ( !Record.NULL_REFERENCE.is( nextProp ) )
        {
            PropertyRecord record = propertyRecordAccess.getIfLoaded( nextProp ).forReadingLinkage();
            result.add( record );
            nextProp = record.getNextProp();
        }
        return result.toArray( new PropertyRecord[result.size()] );
    }

    private Iterator<PropertyBlock> properties( PropertyStore propertyStore, int propertyCount )
    {
        return new PrefetchingIterator<PropertyBlock>()
        {
            private int i;

            @Override
            protected PropertyBlock fetchNextOrNull()
            {
                if ( i >= propertyCount )
                {
                    return null;
                }
                PropertyBlock block = new PropertyBlock();
                propertyStore.encodeValue( block, i, Values.of( random.propertyValue() ) );
                i++;
                return block;
            }
        };
    }

    private static long[] labelIds( int labelCount )
    {
        long[] result = new long[labelCount];
        for ( int i = 0; i < labelCount; i++ )
        {
            result[i] = i;
        }
        return result;
    }

    private static void startAndAwaitCompletionOf( DeleteDuplicateNodesStep step ) throws InterruptedException
    {
        step.start( 0 );
        step.receive( 0, null );
        while ( !step.isCompleted() )
        {
            Thread.sleep( 10 );
        }
    }
}
