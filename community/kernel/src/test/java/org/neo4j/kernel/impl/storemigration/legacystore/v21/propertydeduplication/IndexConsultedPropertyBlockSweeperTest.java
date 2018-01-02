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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexConsultedPropertyBlockSweeperTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( IndexLookupTest.class );

    private GraphDatabaseAPI api;
    private long nodeId;
    private NodeStore nodeStore;
    private PropertyStore propertyStore;
    private Map<String,Integer> propertyKeys;
    private String nonIndexedPropKey;
    private String indexedPropKey;
    private IndexConsultedPropertyBlockSweeper sweeper;
    private IndexLookup.Index indexMock;
    private DuplicatePropertyRemover propertyRemoverMock;
    private String indexedValue;
    private String nonIndexedValue;
    private long propertyId;
    private NodeRecord nodeRecord;

    @Before
    public void setUp() throws IOException
    {
        api = dbRule.getGraphDatabaseAPI();

        nonIndexedPropKey = "notIndexed";
        indexedPropKey = "indexed";

        Label usedLabel = DynamicLabel.label( "UsedLabel" );

        try ( Transaction transaction = api.beginTx() )
        {
            api.schema().indexFor( usedLabel ).on( indexedPropKey ).create();
            transaction.success();
        }

        try ( Transaction transaction = api.beginTx() )
        {
            indexedValue = "value1";
            nonIndexedValue = "value2";

            Node nodeA = api.createNode( usedLabel );
            nodeA.setProperty( indexedPropKey, indexedValue );
            nodeA.setProperty( nonIndexedPropKey, nonIndexedValue );
            nodeId = nodeA.getId();

            transaction.success();
        }

        DependencyResolver resolver = api.getDependencyResolver();
        NeoStoresSupplier neoStoresSupplier = resolver.resolveDependency( NeoStoresSupplier.class );
        NeoStores neoStores = neoStoresSupplier.get();
        nodeStore = neoStores.getNodeStore();
        PropertyKeyTokenStore propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
        propertyKeys = PropertyDeduplicatorTestUtil.indexPropertyKeys( propertyKeyTokenStore );

        propertyStore = neoStores.getPropertyStore();
        nodeRecord = nodeStore.getRecord( nodeId );
        propertyId = nodeRecord.getNextProp();

        indexMock = mock( IndexLookup.Index.class );
        when( indexMock.contains( nodeId, indexedValue ) ).thenReturn( true );

        propertyRemoverMock = mock( DuplicatePropertyRemover.class );
    }

    @Test
    public void shouldNotRemoveIndexedValue() throws Exception
    {
        int propertyKeyId = propertyKeys.get( indexedPropKey );

        sweeper = new IndexConsultedPropertyBlockSweeper( propertyKeyId, indexMock, nodeRecord, propertyStore, propertyRemoverMock );
        assertFalse( sweeper.visited( propertyId ) );

        // Verify that the property is still there
        PropertyRecord propertyRecord = propertyStore.getRecord( propertyId );
        assertNotNull( propertyRecord.getPropertyBlock( propertyKeyId ) );
    }

    @Test
    public void shouldRemoveNonIndexedValue() throws Exception
    {
        int propertyKeyId = propertyKeys.get( nonIndexedPropKey );

        sweeper = new IndexConsultedPropertyBlockSweeper( propertyKeyId, indexMock, nodeRecord, propertyStore, propertyRemoverMock );
        assertFalse( sweeper.visited( propertyId ) );

        // Verify that the property block was removed
        PropertyRecord propertyRecord = propertyStore.getRecord( propertyId );
        assertNull( propertyRecord.getPropertyBlock( propertyKeyId ) );
    }

    @Test
    public void shouldFixThePropertyChainAfterAllTheBlocksInRecordAreRemoved() throws IOException
    {
        int propertyKeyId = propertyKeys.get( indexedPropKey );
        PropertyRecord propertyRecord = propertyStore.getRecord( propertyId );
        for ( PropertyBlock propertyBlock : propertyRecord )
        {
            long[] valueBlocks = propertyBlock.getValueBlocks();
            valueBlocks[1] += 2; // Change the value to something non-indexed!
            propertyBlock.setKeyIndexId( propertyKeyId );
        }
        propertyStore.updateRecord( propertyRecord );

        sweeper = new IndexConsultedPropertyBlockSweeper( propertyKeyId, indexMock, nodeRecord, propertyStore, propertyRemoverMock );
        assertFalse( sweeper.visited( propertyId ) );

        // The property record was emptied of property blocks, so the chain must be fixed
        verify( propertyRemoverMock ).fixUpPropertyLinksAroundUnusedRecord( nodeRecord, propertyRecord );
    }
}
