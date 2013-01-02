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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.*;

public class TestTransactionEventDeadlocks
{
    @Rule
    public EmbeddedDatabaseRule database = new EmbeddedDatabaseRule();
    
    @Test
    public void canAvoidDeadlockThatWouldHappenIfTheRelationshipTypeCreationTransactionModifiedData() throws Exception
    {
        GraphDatabaseService graphdb = database.getGraphDatabaseService();
        final Node root = graphdb.getReferenceNode();
        Transaction tx = graphdb.beginTx();
        try
        {
            root.setProperty( "counter", Long.valueOf( 0L ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        graphdb.registerTransactionEventHandler( new TransactionEventHandler<Void>()
        {
            @SuppressWarnings( "boxing" )
            @Override
            public Void beforeCommit( TransactionData data ) throws Exception
            {
                root.setProperty( "counter", ( (Long) root.removeProperty( "counter" ) ) + 1 );
                return null;
            }

            @Override
            public void afterCommit( TransactionData data, Void state )
            {
                // nothing
            }

            @Override
            public void afterRollback( TransactionData data, Void state )
            {
                // nothing
            }
        } );

        tx = graphdb.beginTx();
        try
        {
            root.setProperty( "state", "not broken yet" );
            root.createRelationshipTo( graphdb.createNode(), DynamicRelationshipType.withName( "TEST" ) );
            root.removeProperty( "state" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        assertEquals( 1L, root.getProperty( "counter" ) );
    }
}
