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
package org.neo4j.kernel.impl.event;

import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.Index;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterables.single;

public class TestTransactionEventsWithIndexes extends TestTransactionEvents
{
    @Test
    public void nodeCanBeLegacyIndexedInBeforeCommit() throws Exception
    {
        // Given we have a legacy index...
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        final Index<Node> index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.index().forNodes( "index" );
            tx.success();
        }

        // ... and a transaction event handler that likes to add nodes to that index
        db.registerTransactionEventHandler( new TransactionEventHandler<Object>()
        {
            @Override
            public Object beforeCommit( TransactionData data ) throws Exception
            {
                Iterator<Node> nodes = data.createdNodes().iterator();
                if ( nodes.hasNext() )
                {
                    Node node = nodes.next();
                    index.add( node, "key", "value" );
                }
                return null;
            }

            @Override
            public void afterCommit( TransactionData data, Object state )
            {}

            @Override
            public void afterRollback( TransactionData data, Object state )
            {}
        } );

        // When we create a node...
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            Node node = db.createNode();
            node.setProperty( "random", 42 );
            tx.success();
        }

        // Then we should be able to look it up through the index.
        try ( Transaction ignore = db.beginTx() )
        {
            Node node = single( index.get( "key", "value" ) );
            assertThat( node.getProperty( "random" ), is( (Object) 42 ) );
        }
    }
}
