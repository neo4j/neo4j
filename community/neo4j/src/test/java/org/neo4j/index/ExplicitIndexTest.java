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
package org.neo4j.index;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ExplicitIndexTest
{
    private static final long TEST_TIMEOUT = 80_000;

    @Rule
    public TestDirectory directory = TestDirectory.testDirectory();

    @Test( timeout = TEST_TIMEOUT )
    public void explicitIndexPopulationWithBunchOfFields() throws Exception
    {
        BatchInserter batchNode = BatchInserters.inserter( directory.graphDbDir() );
        LuceneBatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( batchNode );
        try
        {
            BatchInserterIndex batchIndex = provider.nodeIndex( "node_auto_index",
                    stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );

            Map<String,Object> properties = new HashMap<>();
            for ( int i = 0; i < 2000; i++ )
            {
                properties.put( Integer.toString( i ), RandomStringUtils.randomAlphabetic( 200 ) );
            }

            long node = batchNode.createNode( properties, Label.label( "NODE" ) );
            batchIndex.add( node, properties );
        }
        finally
        {
            provider.shutdown();
            batchNode.shutdown();
        }
    }

    @Test
    public void shouldBeAbleToGetSingleHitAfterCallToHasNext()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            // given
            Index<Node> nodeIndex;
            Index<Relationship> relationshipIndex;
            try ( Transaction tx = db.beginTx() )
            {
                nodeIndex = db.index().forNodes( "MyIndex" );
                relationshipIndex = db.index().forRelationships( "MyIndex" );
                tx.success();
            }
            String key = "key";
            String value = "value";
            Node node;
            Relationship relationship;
            try ( Transaction tx = db.beginTx() )
            {
                node = db.createNode();
                nodeIndex.add( node, key, value );

                relationship = node.createRelationshipTo( node, MyRelTypes.TEST );
                relationshipIndex.add( relationship, key, value );
                tx.success();
            }
            assertFindSingleHit( db, nodeIndex, key, value, node );
            assertFindSingleHit( db, relationshipIndex, key, value, relationship );
        }
        finally
        {
            db.shutdown();
        }
    }

    private <T extends PropertyContainer> void assertFindSingleHit( GraphDatabaseService db, Index<T> nodeIndex, String key, String value, T entity )
    {
        // when get using hasNext + next, then
        assertEquals( entity, findSingle( db, nodeIndex, key, value, hits ->
        {
            assertTrue( hits.hasNext() );
            T result = hits.next();
            assertFalse( hits.hasNext() );
            return result;
        } ) );
        // when get using getSingle, then
        assertEquals( entity, findSingle( db, nodeIndex, key, value, hits ->
        {
            T result = hits.getSingle();
            assertFalse( hits.hasNext() );
            return result;
        } ) );
        // when get using hasNext + getSingle, then
        assertEquals( entity, findSingle( db, nodeIndex, key, value, hits ->
        {
            assertTrue( hits.hasNext() );
            T result = hits.getSingle();
            assertFalse( hits.hasNext() );
            return result;
        } ) );
    }

    private <T extends PropertyContainer> T findSingle( GraphDatabaseService db, Index<T> index, String key, String value, Function<IndexHits<T>,T> getter )
    {
        try ( Transaction tx = db.beginTx() )
        {
            try ( IndexHits<T> hits = index.get( key, value ) )
            {
                T entity = getter.apply( hits );
                tx.success();
                return entity;
            }
        }
    }
}
