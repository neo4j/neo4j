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
package org.neo4j.kernel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

@ExtendWith( ImpermanentDatabaseExtension.class )
public class TestTransactionEventDeadlocks
{
    @Resource
    public ImpermanentDatabaseRule database;

    @Test
    public void canAvoidDeadlockThatWouldHappenIfTheRelationshipTypeCreationTransactionModifiedData()
    {
        GraphDatabaseService graphdb = database.getGraphDatabaseAPI();
        Node node = null;
        try ( Transaction tx = graphdb.beginTx() )
        {
            node = graphdb.createNode();
            node.setProperty( "counter", 0L );
            tx.success();
        }

        graphdb.registerTransactionEventHandler( new RelationshipCounterTransactionEventHandler( node ) );

        try ( Transaction tx = graphdb.beginTx() )
        {
            node.setProperty( "state", "not broken yet" );
            node.createRelationshipTo( graphdb.createNode(), RelationshipType.withName( "TEST" ) );
            node.removeProperty( "state" );
            tx.success();
        }

        assertThat( node, inTx( graphdb, hasProperty( "counter" ).withValue( 1L ) ) );
    }

    private static class RelationshipCounterTransactionEventHandler implements TransactionEventHandler<Void>
    {
        private final Node node;

        RelationshipCounterTransactionEventHandler( Node node )
        {
            this.node = node;
        }

        @SuppressWarnings( "boxing" )
        @Override
        public Void beforeCommit( TransactionData data )
        {
            // TODO Hmm, makes me think... should we really call transaction event handlers
            // for these relationship type / property index transasctions?
            if ( Iterables.count( data.createdRelationships() ) == 0 )
            {
                return null;
            }

            node.setProperty( "counter", ((Long) node.removeProperty( "counter" )) + 1 );
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
    }
}
