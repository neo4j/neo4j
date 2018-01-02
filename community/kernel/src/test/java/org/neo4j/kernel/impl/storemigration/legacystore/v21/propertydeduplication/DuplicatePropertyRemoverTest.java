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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class DuplicatePropertyRemoverTest
{
    @ClassRule
    public static TargetDirectory.TestDirectory storePath = TargetDirectory.testDirForTest( IndexLookupTest.class );

    private static int PROPERTY_COUNT = 1_000;
    private static GraphDatabaseAPI api;
    private static Node node;
    private static long nodeId;
    private static NodeStore nodeStore;
    private static List<String> propertyNames;
    private static Map<String,Integer> indexedPropertyKeys;
    private static PropertyStore propertyStore;
    private static DuplicatePropertyRemover remover;

    @BeforeClass
    public static void setUp()
    {
        GraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        GraphDatabaseService db = factory.newEmbeddedDatabase( storePath.absolutePath() );
        api = (GraphDatabaseAPI) db;

        Label nodeLabel = DynamicLabel.label( "Label" );
        propertyNames = new ArrayList<>();

        try ( Transaction transaction = db.beginTx() )
        {
            node = db.createNode( nodeLabel );
            nodeId = node.getId();

            for( int i = 0; i < PROPERTY_COUNT; i ++ )
            {
                String propKey = "key" + i;
                propertyNames.add( propKey );
                String propValue = "value" + i;
                boolean isBigProp = ThreadLocalRandom.current().nextBoolean();
                if( isBigProp )
                {
                    propValue += propValue;
                    propValue += propValue;
                    propValue += propValue;
                    propValue += propValue;
                    propValue += propValue;
                }
                node.setProperty( propKey, propValue );
            }

            transaction.success();
        }
        Collections.shuffle( propertyNames );

        DependencyResolver resolver = api.getDependencyResolver();
        NeoStoresSupplier neoStoresSupplier = resolver.resolveDependency( NeoStoresSupplier.class );
        NeoStores neoStores = neoStoresSupplier.get();
        nodeStore = neoStores.getNodeStore();
        PropertyKeyTokenStore propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
        indexedPropertyKeys = PropertyDeduplicatorTestUtil.indexPropertyKeys( propertyKeyTokenStore );

        propertyStore = neoStores.getPropertyStore();
        remover = new DuplicatePropertyRemover( nodeStore, propertyStore );
    }

    @AfterClass
    public static void tearDown()
    {
        api.shutdown();
    }

    @Test
    public void shouldRemovePropertyFromLinkedChain() throws Exception
    {
        int prevProBlockCount = propertyNames.size();
        for ( String propertyName : propertyNames )
        {
            // find the property key in question
            int propertyKeyId = indexedPropertyKeys.get( propertyName );

            // remove the property from the node by the given property key
            NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
            removeProperty( nodeRecord, propertyKeyId );

            // check the integrity of the property chain:
            //  - it should not have a property with the given property key id, and have one less property
            assertPropertyRemoved( nodeRecord, prevProBlockCount, propertyKeyId );
            prevProBlockCount--;

            //  - it should not have a cycle
            assertFalse( hasLoop( nodeRecord.getNextProp() ) );
        }
    }

    private void assertPropertyRemoved( NodeRecord nodeRecord, int prevProBlockCount, int propertyKeyId )
    {
        long nextPropId = nodeRecord.getNextProp();
        int propBlockCount = 0;
        while( nextPropId != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propertyStore.getRecord( nextPropId );
            PropertyBlock propertyBlock = propRecord.getPropertyBlock( propertyKeyId );
            assertNull( propertyBlock );
            nextPropId = propRecord.getNextProp();

            propBlockCount += count( (Iterable<PropertyBlock>) propRecord );
        }
        assertEquals( prevProBlockCount - 1, propBlockCount );
    }

    private boolean hasLoop( long firstId )
    {
        PropertyRecord slow, fast, first;
        if ( firstId == Record.NO_NEXT_PROPERTY.intValue() )
        {
            return false;
        }
        first = propertyStore.getRecord( firstId );

        slow = fast = first;

        while ( true )
        {
            // if the fast pointer ever catch up with slow, it indicates a loop.
            slow = getNextPropertyRecord( slow );

            PropertyRecord nextFast = getNextPropertyRecord( fast );
            if ( nextFast != null )
            {
                fast = getNextPropertyRecord( nextFast );
            }
            else
            {
                return false;
            }
            if ( slow == null || fast == null )
            {
                return false;
            }
            if ( slow.getId() == fast.getId() )
            {
                return true;
            }
        }
    }

    private PropertyRecord getNextPropertyRecord( PropertyRecord propRecord )
    {
        long nextPropId = propRecord.getNextProp();
        if ( nextPropId != Record.NO_NEXT_PROPERTY.intValue() )
        {
            propRecord = propertyStore.getRecord( nextPropId );
        }
        else
        {
            propRecord = null;
        }
        return propRecord;
    }

    private void removeProperty( NodeRecord nodeRecord, int propertyKeyId )
    {
        long nextProp = nodeRecord.getNextProp();
        assertTrue( nextProp != Record.NO_NEXT_PROPERTY.intValue() );
        boolean found = false;
        while( nextProp != Record.NO_NEXT_PROPERTY.intValue() && !found )
        {
            PropertyRecord propertyRecord = propertyStore.getRecord( nextProp );
            PropertyBlock propertyBlock = propertyRecord.removePropertyBlock( propertyKeyId );
            if( propertyBlock != null )
            {
                found = true;
                propertyStore.updateRecord( propertyRecord );

                if ( !propertyRecord.iterator().hasNext() )
                {
                    remover.fixUpPropertyLinksAroundUnusedRecord( nodeRecord, propertyRecord );
                }
            }
            nextProp = propertyRecord.getNextProp();
        }
        assertTrue( found );
    }
}
