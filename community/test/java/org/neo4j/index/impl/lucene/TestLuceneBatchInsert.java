/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertTrue;
import static org.neo4j.index.Neo4jTestCase.assertContains;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class TestLuceneBatchInsert
{
    private static final String PATH = "target/var/batch";

    @Before
    public void cleanDirectory()
    {
        Neo4jTestCase.deleteFileOrDirectory( new File( PATH ) );
    }

    @Test
    public void testSome() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex index = provider.nodeIndex( "users",
                LuceneIndexProvider.EXACT_CONFIG );
        Map<Integer, Long> ids = new HashMap<Integer, Long>();
        for ( int i = 0; i < 100; i++ )
        {
            long id = inserter.createNode( null );
            index.add( id, MapUtil.map( "name", "Joe" + i, "other", "Schmoe" ) );
            ids.put( i, id );
        }

        for ( int i = 0; i < 100; i++ )
        {
            assertContains( index.get( "name", "Joe" + i ), ids.get( i ) );
        }
        assertContains( index.query( "name:Joe0 AND other:Schmoe" ),
                ids.get( 0 ) );

        assertContains( index.query( "name", "Joe*" ),
                ids.values().toArray( new Long[ids.size()] ) );
        provider.shutdown();
        inserter.shutdown();

        GraphDatabaseService db = new EmbeddedGraphDatabase( PATH );
        assertTrue( db.index().existsForNodes( "users" ) );
        Index<Node> dbIndex = db.index().forNodes( "users" );
        for ( int i = 0; i < 100; i++ )
        {
            assertContains( dbIndex.get( "name", "Joe" + i ),
                    db.getNodeById( ids.get( i ) ) );
        }

        Collection<Node> nodes = new ArrayList<Node>();
        for ( long id : ids.values() )
        {
            nodes.add( db.getNodeById( id ) );
        }
        assertContains( dbIndex.query( "name", "Joe*" ),
                nodes.toArray( new Node[nodes.size()] ) );
        assertContains( dbIndex.query( "name:Joe0 AND other:Schmoe" ),
                db.getNodeById( ids.get( 0 ) ) );
        db.shutdown();
    }

    @Test
    public void testFulltext()
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        String name = "users";
        BatchInserterIndex index = provider.nodeIndex( name,
                MapUtil.stringMap( "type", "fulltext" ) );

        long id1 = inserter.createNode( null );
        index.add( id1, MapUtil.map( "name", "Mattias Persson", "email",
                "something@somewhere", "something", "bad" ) );
        long id2 = inserter.createNode( null );
        index.add( id2, MapUtil.map( "name", "Lars PerssoN" ) );
        index.flush();
        assertContains( index.get( "name", "Mattias Persson" ), id1 );
        assertContains( index.query( "name", "mattias" ), id1 );
        assertContains( index.query( "name", "bla" ) );
        assertContains( index.query( "name", "persson" ), id1, id2 );
        assertContains( index.query( "email", "*@*" ), id1 );
        assertContains( index.get( "something", "bad" ), id1 );
        long id3 = inserter.createNode( null );
        index.add( id3,
                MapUtil.map( "name", new String[] { "What Ever", "Anything" } ) );
        index.flush();
        assertContains( index.get( "name", "What Ever" ), id3 );
        assertContains( index.get( "name", "Anything" ), id3 );

        provider.shutdown();
        inserter.shutdown();

        GraphDatabaseService db = new EmbeddedGraphDatabase( PATH );
        Index<Node> dbIndex = db.index().forNodes( name );
        Node node1 = db.getNodeById( id1 );
        Node node2 = db.getNodeById( id2 );
        assertContains( dbIndex.query( "name", "persson" ), node1, node2 );
        db.shutdown();
    }

    @Ignore
    @Test
    public void testInsertionSpeed()
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex index = provider.nodeIndex( "yeah",
                LuceneIndexProvider.EXACT_CONFIG );
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 100000; i++ )
        {
            long id = inserter.createNode( null );
            index.add( id, MapUtil.map( "key", "value" + i ) );
        }
        System.out.println( "insert:" + ( System.currentTimeMillis() - t ) );
        index.flush();

        t = System.currentTimeMillis();
        for ( int i = 0; i < 10000; i++ )
        {
            for ( long n : index.get( "key", "value" + i ) )
            {
            }
        }
        System.out.println( "get:" + ( System.currentTimeMillis() - t ) );
    }

    @Test
    public void testFindCreatedIndex()
    {
        String indexName = "persons";
        BatchInserter inserter = new BatchInserterImpl( PATH );
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex persons = indexProvider.nodeIndex( "persons",
                MapUtil.stringMap( "type", "exact" ) );
        Map<String, Object> properties = MapUtil.map( "name", "test" );
        long node = inserter.createNode( properties );
        persons.add( node, properties );
        indexProvider.shutdown();
        inserter.shutdown();
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( PATH );
        Transaction tx = graphDb.beginTx();
        try
        {
            IndexManager indexManager = graphDb.index();
            Assert.assertFalse( indexManager.existsForRelationships( indexName ) );
            Assert.assertTrue( indexManager.existsForNodes( indexName ) );
            Assert.assertNotNull( indexManager.forNodes( indexName ) );
            Index<Node> nodes = graphDb.index().forNodes( indexName );
            Assert.assertTrue( nodes.get(
                    "name", "test" ).hasNext() );
            tx.success();
            tx.finish();
        }
        finally
        {
            graphDb.shutdown();
        }
    }
}
