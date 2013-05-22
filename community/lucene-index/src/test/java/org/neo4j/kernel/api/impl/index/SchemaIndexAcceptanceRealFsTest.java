/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.test.TargetDirectory.forTest;

public class SchemaIndexAcceptanceRealFsTest
{
    @Test
    public void shouldDropIndexAfterQueriedOutsideTransaction() throws Exception
    {
        // GIVEN
        String propertyKey = "key";
        IndexDefinition index = createIndex( label, propertyKey );
        createSomeData( label, propertyKey );
        
        ResourceIterable<Node> result = db.findNodesByLabelAndProperty( label, propertyKey, "yeah" );
        ResourceIterator<Node> iterator = result.iterator();
        iterator.close();

        // WHEN
        dropIndex( index );
        
        // THEN
        assertEquals( emptySetOf( IndexDefinition.class ), asSet( db.schema().getIndexes() ) );
    }
 
    private GraphDatabaseService db;
    private final Label label = label( "PERSON" );

    @Before
    public void before() throws Exception
    {
        db = newDb();
    }

    private GraphDatabaseService newDb()
    {
        return new GraphDatabaseFactory().newEmbeddedDatabase(
                forTest( getClass() ).graphDbDir( true ).getAbsolutePath() );
    }
    
    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private void dropIndex( IndexDefinition indexDefinition )
    {
        Transaction tx = db.beginTx();
        indexDefinition.drop();
        tx.success();
        tx.finish();
    }

    private IndexDefinition createIndex( Label label, String propertyKey )
    {
        Transaction tx = db.beginTx();
        IndexDefinition indexDefinition = db.schema().indexFor( label ).on( propertyKey ).create();
        tx.success();
        tx.finish();
        db.schema().awaitIndexOnline( indexDefinition, 1, MINUTES );
        return indexDefinition;
    }

    private void createSomeData( Label label, String propertyKey )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode( label );
            node.setProperty( propertyKey, "yeah" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
