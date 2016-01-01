/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TargetDirectory;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestRelationshipGroupStore
{
    private File directory;
    private String neostoreFileName;
    private int defaultThreshold;
    private FileSystemAbstraction fs;
    private GraphDatabaseAPI db;

    @Before
    public void before() throws Exception
    {
        directory = TargetDirectory.forTest( getClass() ).makeGraphDbDir();
        neostoreFileName = new File( directory, "neostore" ).getAbsolutePath();
        defaultThreshold = parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() );
    }

    @Test
    public void createWithDefaultThreshold() throws Exception
    {
        createAndVerify( null );
    }

    @Test
    public void createWithCustomThreshold() throws Exception
    {
        createAndVerify( defaultThreshold*2 );
    }

    @Test
    public void createDenseNodeWithLowThreshold() throws Exception
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
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( NodeImpl.class, db.getDependencyResolver().resolveDependency( NodeManager.class )
                    .getNodeForProxy( node.getId() ).getClass() );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
        }

        db.shutdown();
    }

    private void newDb( int denseNodeThreshold )
    {
        db = new ImpermanentGraphDatabase( MapUtil.stringMap( "dense_node_threshold", "" + denseNodeThreshold ) )
        {
            @Override
            protected FileSystemAbstraction createFileSystemAbstraction()
            {
                return (fs = super.createFileSystemAbstraction());
            }
        };
    }

    private void createAndVerify( Integer customThreshold )
    {
        int expectedThreshold = customThreshold != null ? customThreshold : defaultThreshold;
        StoreFactory factory = factory( customThreshold );
        NeoStore neoStore = factory.createNeoStore( new File( neostoreFileName ) );
        assertEquals( expectedThreshold, neoStore.getDenseNodeThreshold() );
        neoStore.close();

        // Next time we open it it should be the same
        neoStore = factory.newNeoStore( new File( neostoreFileName ) );
        assertEquals( expectedThreshold, neoStore.getDenseNodeThreshold() );
        neoStore.close();

        // Even if we open with a different config setting it should just ignore it
        factory = factory( 999999 );
        neoStore = factory.newNeoStore( new File( neostoreFileName ) );
        assertEquals( expectedThreshold, neoStore.getDenseNodeThreshold() );
        neoStore.close();
    }

    private StoreFactory factory( Integer customThreshold )
    {
        Map<String, String> customConfig = new HashMap<>();
        if ( customThreshold != null )
        {
            customConfig.put( GraphDatabaseSettings.dense_node_threshold.name(), "" + customThreshold );
        }
        return new StoreFactory( config( customConfig ), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL,
                new DefaultTxHook() );
    }

    private Config config( Map<String, String> customConfig )
    {
        return new Config( customConfig );
    }

    @Test
    public void makeSureRelationshipGroupsNextAndPrevGetsAssignedCorrectly() throws Exception
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
            tx.success();
        }

        db.shutdown();
    }

    @Test
    public void verifyRecordsForDenseNodeWithOneRelType() throws Exception
    {
        // TODO test on a lower level instead

        newDb( 2 );

        Node node;
        Relationship rel1, rel2, rel3, rel4, rel5, rel6;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            rel1 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel2 = db.createNode().createRelationshipTo( node, MyRelTypes.TEST );
            rel3 = node.createRelationshipTo( node, MyRelTypes.TEST );
            rel4 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel5 = db.createNode().createRelationshipTo( node, MyRelTypes.TEST );
            rel6 = node.createRelationshipTo( node, MyRelTypes.TEST );
            tx.success();
        }

        NeoStore neoStore = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getNeoStoreDataSource().getNeoStore();
        NodeStore nodeStore = neoStore.getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( node.getId() );
        long group = nodeRecord.getNextRel();
        RelationshipGroupStore groupStore = neoStore.getRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.getRecord( group );
        assertEquals( -1, groupRecord.getNext() );
        assertEquals( -1, groupRecord.getPrev() );
        assertRelationshipChain( neoStore.getRelationshipStore(), node, groupRecord.getFirstOut(), rel1.getId(), rel4.getId() );
        assertRelationshipChain( neoStore.getRelationshipStore(), node, groupRecord.getFirstIn(), rel2.getId(), rel5.getId() );
        assertRelationshipChain( neoStore.getRelationshipStore(), node, groupRecord.getFirstLoop(), rel3.getId(), rel6.getId() );
    }

    @Test
    public void verifyRecordsForDenseNodeWithTwoRelTypes() throws Exception
    {
        // TODO test on a lower level instead

        newDb( 2 );

        Node node;
        Relationship rel1, rel2, rel3, rel4, rel5, rel6;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            rel1 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel2 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel3 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            rel4 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            rel5 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            rel6 = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            tx.success();
        }

        NeoStore neoStore = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getNeoStoreDataSource().getNeoStore();
        NodeStore nodeStore = neoStore.getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( node.getId() );
        long group = nodeRecord.getNextRel();

        RelationshipGroupStore groupStore = neoStore.getRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.getRecord( group );
        assertFalse( groupRecord.getNext() == -1 );
        assertRelationshipChain( neoStore.getRelationshipStore(), node, groupRecord.getFirstOut(), rel1.getId(), rel2.getId(), rel3.getId() );

        RelationshipGroupRecord otherGroupRecord = groupStore.getRecord( groupRecord.getNext() );
        assertEquals( -1, otherGroupRecord.getNext() );
        assertRelationshipChain( neoStore.getRelationshipStore(), node, otherGroupRecord.getFirstOut(), rel4.getId(), rel5.getId(), rel6.getId() );
    }

    @Test
    public void verifyGroupIsDeletedWhenNeeded() throws Exception
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
        tx.success();
        tx.finish();

        NeoStore neoStore = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getNeoStoreDataSource().getNeoStore();
        NodeStore nodeStore = neoStore.getNodeStore();
        NodeRecord nodeRecord = nodeStore.getRecord( node.getId() );
        long group = nodeRecord.getNextRel();

        RelationshipGroupStore groupStore = neoStore.getRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.getRecord( group );
        assertFalse( groupRecord.getNext() == -1 );
        RelationshipGroupRecord otherGroupRecord = groupStore.getRecord( groupRecord.getNext() );
        assertEquals( -1, otherGroupRecord.getNext() );

        // TODO Delete all relationships of one type and see to that the correct group is deleted.
    }

    private void assertRelationshipChain( RelationshipStore relationshipStore, Node node, long firstId, long... chainedIds )
    {
        long nodeId = node.getId();
        RelationshipRecord record = relationshipStore.getRecord( firstId );
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
            record = relationshipStore.getRecord( nextId );
        }

        Set<Long> expectedChain = new HashSet<>( asList( firstId ) );
        for ( long id : chainedIds )
        {
            expectedChain.add( id );
        }
        assertEquals( expectedChain, readChain );
    }
}
