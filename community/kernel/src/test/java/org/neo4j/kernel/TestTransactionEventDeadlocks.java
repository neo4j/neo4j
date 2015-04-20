/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class TestTransactionEventDeadlocks
{
    @Rule
    public DatabaseRule database = new ImpermanentDatabaseRule();
    
    @Test
    public void canAvoidDeadlockThatWouldHappenIfTheRelationshipTypeCreationTransactionModifiedData() throws Exception
    {
        GraphDatabaseService graphdb = database.getGraphDatabaseService();
        Transaction tx = graphdb.beginTx();
        final Node root = graphdb.createNode();
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
                // TODO Hmm, makes me think... should we really call transaction event handlers
                // for these relationship type / property index transasctions?
                if ( count( data.createdRelationships() ) == 0 )
                    return null;
                
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

        assertThat( root, inTx( graphdb, hasProperty( "counter" ).withValue( 1L ) ) );
    }
}
