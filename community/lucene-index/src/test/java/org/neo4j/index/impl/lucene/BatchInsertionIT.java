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
package org.neo4j.index.impl.lucene;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;

public class BatchInsertionIT
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( BatchInsertionIT.class ).startLazily();

    @Test
    public void shouldIndexNodesWithMultipleLabels() throws Exception
    {
        // Given
        String path = dbRule.getStoreDirAbsolutePath();
        BatchInserter inserter = BatchInserters.inserter( path );

        inserter.createNode( map( "name", "Bob" ), label( "User" ), label( "Admin" ) );

        inserter.createDeferredSchemaIndex( label( "User" ) ).on( "name" ).create();
        inserter.createDeferredSchemaIndex( label( "Admin" ) ).on( "name" ).create();

        // When
        inserter.shutdown();

        // Then
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( db.findNodes( label( "User" ), "name", "Bob" ) ), equalTo(1) );
            assertThat( count( db.findNodes( label( "Admin" ), "name", "Bob" ) ), equalTo(1) );
        }
        finally
        {
            db.shutdown();
        }

    }

    @Test
    public void shouldNotIndexNodesWithWrongLabel() throws Exception
    {
        // Given
        BatchInserter inserter = BatchInserters.inserter( dbRule.getStoreDirAbsolutePath() );

        inserter.createNode( map("name", "Bob"), label( "User" ), label("Admin"));

        inserter.createDeferredSchemaIndex( label( "Banana" ) ).on( "name" ).create();

        // When
        inserter.shutdown();

        // Then
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try(Transaction tx = db.beginTx())
        {
            assertThat( count( db.findNodes( label( "Banana" ), "name", "Bob" ) ), equalTo(0) );
        }
        finally
        {
            db.shutdown();
        }

    }

    @Test
    public void shouldBeAbleToMakeRepeatedCallsToSetNodeProperty() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( dbRule.getStoreDirAbsolutePath() );
        long nodeId = inserter.createNode( Collections.<String, Object>emptyMap() );

        final Object finalValue = 87;
        inserter.setNodeProperty( nodeId, "a", "some property value" );
        inserter.setNodeProperty( nodeId, "a", 42 );
        inserter.setNodeProperty( nodeId, "a", 3.14 );
        inserter.setNodeProperty( nodeId, "a", true );
        inserter.setNodeProperty( nodeId, "a", finalValue );
        inserter.shutdown();

        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try(Transaction ignored = db.beginTx())
        {
            assertThat( db.getNodeById( nodeId ).getProperty( "a" ), equalTo( finalValue ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldBeAbleToMakeRepeatedCallsToSetNodePropertyWithMultiplePropertiesPerBlock() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( dbRule.getStoreDirAbsolutePath() );
        long nodeId = inserter.createNode( Collections.<String, Object>emptyMap() );

        final Object finalValue1 = 87;
        final Object finalValue2 = 3.14;
        inserter.setNodeProperty( nodeId, "a", "some property value" );
        inserter.setNodeProperty( nodeId, "a", 42 );
        inserter.setNodeProperty( nodeId, "b", finalValue2 );
        inserter.setNodeProperty( nodeId, "a", finalValue2 );
        inserter.setNodeProperty( nodeId, "a", true );
        inserter.setNodeProperty( nodeId, "a", finalValue1 );
        inserter.shutdown();

        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try(Transaction ignored = db.beginTx())
        {
            assertThat( db.getNodeById( nodeId ).getProperty( "a" ), equalTo( finalValue1 ) );
            assertThat( db.getNodeById( nodeId ).getProperty( "b" ), equalTo( finalValue2 ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Ignore
    @Test
    public void testInsertionSpeed() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( dbRule.getStoreDirAbsolutePath() );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex index = provider.nodeIndex( "yeah", EXACT_CONFIG );
        index.setCacheCapacity( "key", 1000000 );
        long t = currentTimeMillis();
        for ( int i = 0; i < 1000000; i++ )
        {
            Map<String, Object> properties = map( "key", "value" + i );
            long id = inserter.createNode( properties );
            index.add( id, properties );
        }
        System.out.println( "insert:" + ( currentTimeMillis() - t ) );
        index.flush();

        t = currentTimeMillis();
        for ( int i = 0; i < 1000000; i++ )
        {
            count( (Iterator<Long>) index.get( "key", "value" + i ) );
        }
        System.out.println( "get:" + ( currentTimeMillis() - t ) );
    }
}
