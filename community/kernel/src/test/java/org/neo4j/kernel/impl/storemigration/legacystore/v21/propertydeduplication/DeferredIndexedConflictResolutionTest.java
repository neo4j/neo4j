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

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.intThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeferredIndexedConflictResolutionTest
{
    @ClassRule
    public static TargetDirectory.TestDirectory storePath = TargetDirectory.testDirForTest( IndexLookupTest.class );
    private NodeRecord nodeRecord;
    private PropertyStore propertyStore;
    private NodeStore nodeStore;
    private List<DuplicateCluster> clusters;
    private DuplicateCluster clusterToRemove;
    private GraphDatabaseService db;

    @Test
    public void shouldRemoveDuplicateClustersForWhichThereIsAnIndex() throws IOException
    {
        // given
        DuplicatePropertyRemover remover = new DuplicatePropertyRemover( nodeStore, propertyStore );
        IndexLookup indexLookup = mock( IndexLookup.class );

        IndexLookup.Index indexStub = new IndexLookup.Index()
        {
            @Override
            public boolean contains( long nodeId, Object propertyValue ) throws IOException
            {
                return true;
            }
        };

        // when
        when( indexLookup.getAnyIndexOrNull(
                any( long[].class ),
                intThat( is( clusterToRemove.propertyKeyId ) ) ) ).thenReturn( indexStub );

        DeferredIndexedConflictResolution resolution = new DeferredIndexedConflictResolution( nodeRecord, clusters,
                nodeStore, indexLookup, propertyStore, remover );

        resolution.resolve();

        // then
        assertThat( clusters, not( contains( sameInstance( clusterToRemove ) ) ) );
    }

    @Before
    public void setUp()
    {
        GraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        db = factory.newEmbeddedDatabase( storePath.absolutePath() );
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;

        Label nodeLabel = DynamicLabel.label( "Label" );
        String propertyKey = "someProp";
        long nodeId;

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = db.createNode( nodeLabel );
            node.setProperty( propertyKey, "someVal" );
            nodeId = node.getId();
            transaction.success();
        }

        DependencyResolver resolver = api.getDependencyResolver();
        NeoStoresSupplier neoStoresSupplier = resolver.resolveDependency( NeoStoresSupplier.class );
        NeoStores neoStores = neoStoresSupplier.get();
        nodeStore = neoStores.getNodeStore();
        propertyStore = neoStores.getPropertyStore();
        Map<String,Integer> propertyKeys =
                PropertyDeduplicatorTestUtil.indexPropertyKeys( neoStores.getPropertyKeyTokenStore() );

        nodeRecord = nodeStore.getRecord( nodeId );
        int propertyKeyId = propertyKeys.get( propertyKey );
        clusterToRemove = createDuplicateCluster( propertyKeyId, nodeRecord.getNextProp() );
        clusters = new ArrayList<>();
        clusters.add( createDuplicateCluster( propertyKeyId + 1, nodeRecord.getNextProp() ) );
        clusters.add( clusterToRemove ); // This is the one we want to remove
        clusters.add( createDuplicateCluster( propertyKeyId + 2, nodeRecord.getNextProp() ) );
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    private DuplicateCluster createDuplicateCluster( int propertyKeyId, long firstPropertyId )
    {
        DuplicateCluster cluster = new DuplicateCluster( propertyKeyId );
        cluster.propertyRecordIds.add( firstPropertyId );
        return cluster;
    }
}
