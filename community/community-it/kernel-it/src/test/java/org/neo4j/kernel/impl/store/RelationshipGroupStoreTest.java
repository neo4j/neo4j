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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.test.rule.PageCacheConfig.config;

@TestDirectoryExtension
class RelationshipGroupStoreTest
{
    @RegisterExtension
    static final PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( config().withInconsistentReads( false ) );
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    private int defaultThreshold;
    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void before()
    {
        defaultThreshold = dense_node_threshold.defaultValue();
    }

    @AfterEach
    void after()
    {
        if ( db != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void createWithDefaultThreshold()
    {
        createAndVerify( null );
    }

    @Test
    void createWithCustomThreshold()
    {
        createAndVerify( defaultThreshold * 2 );
    }

    @Test
    void createDenseNodeWithLowThreshold()
    {
        newDb( 2 );

        // Create node with two relationships
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            assertEquals( 2, node.getDegree() );
            assertEquals( 1, node.getDegree( MyRelTypes.TEST ) );
            assertEquals( 1, node.getDegree( MyRelTypes.TEST2 ) );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.commit();
        }

        managementService.shutdown();
    }

    private void newDb( int denseNodeThreshold )
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent()
                .setConfig( dense_node_threshold, denseNodeThreshold ).build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        fs = db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }

    private void createAndVerify( Integer customThreshold )
    {
        int expectedThreshold = customThreshold != null ? customThreshold : defaultThreshold;
        StoreFactory factory = factory( customThreshold );
        NeoStores neoStores = factory.openAllNeoStores( true );
        assertEquals( expectedThreshold, neoStores.getRelationshipGroupStore().getStoreHeaderInt() );
        neoStores.close();

        // Next time we open it it should be the same
        neoStores = factory.openAllNeoStores();
        assertEquals( expectedThreshold, neoStores.getRelationshipGroupStore().getStoreHeaderInt() );
        neoStores.close();

        // Even if we open with a different config setting it should just ignore it
        factory = factory( 999999 );
        neoStores = factory.openAllNeoStores();
        assertEquals( expectedThreshold, neoStores.getRelationshipGroupStore().getStoreHeaderInt() );
        neoStores.close();
    }

    private StoreFactory factory( Integer customThreshold )
    {
        return factory( customThreshold, pageCacheExtension.getPageCache( fs ) );
    }

    private StoreFactory factory( Integer customThreshold, PageCache pageCache )
    {
        Config.Builder config = Config.newBuilder();
        if ( customThreshold != null )
        {
            config.set( dense_node_threshold, customThreshold );
        }
        return new StoreFactory( testDirectory.databaseLayout(), config.build(), new DefaultIdGeneratorFactory( fs, immediate() ),
                pageCache, fs, NullLogProvider.getInstance() );
    }

    @Test
    void makeSureRelationshipGroupsNextAndPrevGetsAssignedCorrectly()
    {
        newDb( 1 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Node node0 = db.createNode();
            Node node2 = db.createNode();
            node0.createRelationshipTo( node, MyRelTypes.TEST );
            node.createRelationshipTo( node2, MyRelTypes.TEST2 );

            for ( Relationship rel : node.getRelationships() )
            {
                rel.delete();
            }
            node.delete();
            tx.commit();
        }

        managementService.shutdown();
    }

    @Test
    void verifyRecordsForDenseNodeWithOneRelType()
    {
        newDb( 2 );

        Node node;
        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        Relationship rel4;
        Relationship rel5;
        Relationship rel6;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            rel1 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel2 = db.createNode().createRelationshipTo( node, MyRelTypes.TEST );
            rel3 = node.createRelationshipTo( node, MyRelTypes.TEST );
            rel4 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel5 = db.createNode().createRelationshipTo( node, MyRelTypes.TEST );
            rel6 = node.createRelationshipTo( node, MyRelTypes.TEST );
            tx.commit();
        }

        NeoStores neoStores = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( node.getId(), nodeStore.newRecord(), NORMAL );
        long group = nodeRecord.getNextRel();
        RecordStore<RelationshipGroupRecord> groupStore = neoStores.getRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.getRecord( group, groupStore.newRecord(), NORMAL );
        assertEquals( -1, groupRecord.getNext() );
        assertEquals( -1, groupRecord.getPrev() );
        assertRelationshipChain( neoStores.getRelationshipStore(), node, groupRecord.getFirstOut(), rel1.getId(), rel4.getId() );
        assertRelationshipChain( neoStores.getRelationshipStore(), node, groupRecord.getFirstIn(), rel2.getId(), rel5.getId() );
        assertRelationshipChain( neoStores.getRelationshipStore(), node, groupRecord.getFirstLoop(), rel3.getId(), rel6.getId() );
    }

    @Test
    void verifyRecordsForDenseNodeWithTwoRelTypes()
    {
        newDb( 2 );

        Node node;
        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        Relationship rel4;
        Relationship rel5;
        Relationship rel6;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            rel1 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel2 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel3 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel4 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            rel5 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            rel6 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            tx.commit();
        }

        NeoStores neoStores = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( node.getId(), nodeStore.newRecord(), NORMAL );
        long group = nodeRecord.getNextRel();

        RecordStore<RelationshipGroupRecord> groupStore = neoStores.getRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.getRecord( group, groupStore.newRecord(), NORMAL );
        assertNotEquals( groupRecord.getNext(), -1 );
        assertRelationshipChain( neoStores.getRelationshipStore(), node, groupRecord.getFirstOut(), rel1.getId(),
                rel2.getId(), rel3.getId() );

        RelationshipGroupRecord otherGroupRecord = groupStore.getRecord( groupRecord.getNext(), groupStore.newRecord(), NORMAL );
        assertEquals( -1, otherGroupRecord.getNext() );
        assertRelationshipChain( neoStores.getRelationshipStore(), node, otherGroupRecord.getFirstOut(), rel4.getId(),
                rel5.getId(), rel6.getId() );
    }

    @Test
    void verifyGroupIsDeletedWhenNeeded()
    {
        // TODO test on a lower level instead

        newDb( 2 );

        Transaction tx = db.beginTx();
        Node node = db.createNode();
        Relationship rel1 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
        Relationship rel2 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
        Relationship rel3 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
        Relationship rel4 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
        Relationship rel5 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
        Relationship rel6 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
        tx.commit();

        NeoStores neoStores = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( node.getId(), nodeStore.newRecord(), NORMAL );
        long group = nodeRecord.getNextRel();

        RecordStore<RelationshipGroupRecord> groupStore = neoStores.getRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.getRecord( group, groupStore.newRecord(), NORMAL );
        assertNotEquals( groupRecord.getNext(), -1 );
        RelationshipGroupRecord otherGroupRecord = groupStore.getRecord( groupRecord.getNext(), groupStore.newRecord(),
                NORMAL );
        assertEquals( -1, otherGroupRecord.getNext() );

        // TODO Delete all relationships of one type and see to that the correct group is deleted.
    }

    @Test
    void checkingIfRecordIsInUseMustHappenAfterConsistentRead()
    {
        AtomicBoolean nextReadIsInconsistent = new AtomicBoolean( false );
        PageCache pageCache = pageCacheExtension.getPageCache( fs,
                config().withInconsistentReads( nextReadIsInconsistent ) );
        StoreFactory factory = factory( null, pageCache );

        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            RelationshipGroupRecord record = new RelationshipGroupRecord( 1 ).initialize( true, 2, 3, 4, 5, 6,
                    Record.NO_NEXT_RELATIONSHIP.intValue() );
            relationshipGroupStore.updateRecord( record );
            nextReadIsInconsistent.set( true );
            // Now the following should not throw any RecordNotInUse exceptions
            RelationshipGroupRecord readBack = relationshipGroupStore.getRecord( 1, relationshipGroupStore.newRecord(), NORMAL );
            assertThat( readBack.toString(), equalTo( record.toString() ) );
        }
    }

    private static void assertRelationshipChain( RelationshipStore relationshipStore, Node node, long firstId, long... chainedIds )
    {
        long nodeId = node.getId();
        RelationshipRecord record = relationshipStore.getRecord( firstId, relationshipStore.newRecord(), NORMAL );
        Set<Long> readChain = new HashSet<>();
        readChain.add( firstId );
        while ( true )
        {
            long nextId = record.getFirstNode() == nodeId ?
                    record.getFirstNextRel() :
                    record.getSecondNextRel();
            if ( nextId == -1 )
            {
                break;
            }

            readChain.add( nextId );
            relationshipStore.getRecord( nextId, record, NORMAL );
        }

        Set<Long> expectedChain = new HashSet<>( Collections.singletonList( firstId ) );
        for ( long id : chainedIds )
        {
            expectedChain.add( id );
        }
        assertEquals( expectedChain, readChain );
    }
}
