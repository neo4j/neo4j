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

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.RandomRule;

/**
 * Test for randomly creating data and verifying transaction data seen in transaction event handlers.
 */
public class TransactionEventsIT
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();
    public final @Rule RandomRule random = new RandomRule();

    @Test
    public void shouldSeeExpectedTransactionData() throws Exception
    {
        // GIVEN
        final Graph state = new Graph( db, random );
        final ExpectedTransactionData expected = new ExpectedTransactionData( true );
        final TransactionEventHandler<Object> handler = new VerifyingTransactionEventHandler( expected );
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                Operation.createNode.perform( state, expected );
            }
            for ( int i = 0; i < 20; i++ )
            {
                Operation.createRelationship.perform( state, expected );
            }
            tx.success();
        }

        db.registerTransactionEventHandler( handler );

        // WHEN
        Operation[] operations = Operation.values();
        for ( int i = 0; i < 1_000; i++ )
        {
            expected.clear();
            try ( Transaction tx = db.beginTx() )
            {
                int transactionSize = random.intBetween( 1, 20 );
                for ( int j = 0; j < transactionSize; j++ )
                {
                    random.among( operations ).perform( state, expected );
                }
                tx.success();
            }
        }

        // THEN the verifications all happen inside the transaction event handler
    }

    enum Operation
    {
        createNode
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.createNode();
                expectations.createdNode( node );
                debug( node );
            }
        },
        deleteNode
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    for ( Relationship relationship : node.getRelationships() )
                    {
                        graph.deleteRelationship( relationship );
                        expectations.deletedRelationship( relationship );
                        debug( relationship );
                    }
                    graph.deleteNode( node );
                    expectations.deletedNode( node );
                    debug( node );
                }
            }
        },
        assignLabel
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    Label label = graph.randomLabel();
                    if ( !node.hasLabel( label ) )
                    {
                        node.addLabel( label );
                        expectations.assignedLabel( node, label );
                        debug( node + " " + label );
                    }
                }
            }
        },
        removeLabel
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    Label label = graph.randomLabel();
                    if ( node.hasLabel( label ) )
                    {
                        node.removeLabel( label );
                        expectations.removedLabel( node, label );
                        debug( node + " " + label );
                    }
                }
            }
        },
        setNodeProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    String key = graph.randomPropertyKey();
                    Object valueBefore = node.getProperty( key, null );
                    Object value = graph.randomPropertyValue();
                    node.setProperty( key, value );
                    expectations.assignedProperty( node, key, value, valueBefore );
                    debug( node + " " + key + "=" + value + " prev " + valueBefore );
                }
            }
        },
        removeNodeProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    String key = graph.randomPropertyKey();
                    if ( node.hasProperty( key ) )
                    {
                        Object valueBefore = node.removeProperty( key );
                        expectations.removedProperty( node, key, valueBefore );
                        debug( node + " " + key + "=" + valueBefore );
                    }
                }
            }
        },
        setRelationshipProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Relationship relationship = graph.randomRelationship();
                if ( relationship != null )
                {
                    String key = graph.randomPropertyKey();
                    Object valueBefore = relationship.getProperty( key, null );
                    Object value = graph.randomPropertyValue();
                    relationship.setProperty( key, value );
                    expectations.assignedProperty( relationship, key, value, valueBefore );
                    debug( relationship + " " + key + "=" + value + " prev " + valueBefore );
                }
            }
        },
        removeRelationshipProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Relationship relationship = graph.randomRelationship();
                if ( relationship != null )
                {
                    String key = graph.randomPropertyKey();
                    if ( relationship.hasProperty( key ) )
                    {
                        Object valueBefore = relationship.removeProperty( key );
                        expectations.removedProperty( relationship, key, valueBefore );
                        debug( relationship + " " + key + "=" + valueBefore );
                    }
                }
            }
        },
        createRelationship
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                while ( graph.nodeCount() < 2 )
                {
                    createNode.perform( graph, expectations );
                }
                Node node1 = graph.randomNode();
                Node node2 = graph.randomNode();
                Relationship relationship = graph.createRelationship( node1, node2, graph.randomRelationshipType() );
                expectations.createdRelationship( relationship );
                debug( relationship );
            }
        },
        deleteRelationship
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Relationship relationship = graph.randomRelationship();
                if ( relationship != null )
                {
                    graph.deleteRelationship( relationship );
                    expectations.deletedRelationship( relationship );
                    debug( relationship );
                }
            }
        };

        abstract void perform( Graph graph, ExpectedTransactionData expectations );

        void debug( Object value )
        {   // Add a system.out here if you need to debug this case a bit easier
        }
    }

    private static class Graph
    {
        private static final String[] TOKENS = {"A", "B", "C", "D", "E"};

        private final GraphDatabaseService db;
        private final RandomRule random;
        private final List<Node> nodes = new ArrayList<>();
        private final List<Relationship> relationships = new ArrayList<>();

        Graph( GraphDatabaseService db, RandomRule random )
        {
            this.db = db;
            this.random = random;
        }

        private <E extends PropertyContainer> E random( List<E> entities )
        {
            return entities.isEmpty() ? null : entities.get( random.nextInt( entities.size() ) );
        }

        Node randomNode()
        {
            return random( nodes );
        }

        Relationship randomRelationship()
        {
            return random( relationships );
        }

        Node createNode()
        {
            Node node = db.createNode();
            nodes.add( node );
            return node;
        }

        void deleteRelationship( Relationship relationship )
        {
            relationship.delete();
            relationships.remove( relationship );
        }

        void deleteNode( Node node )
        {
            node.delete();
            nodes.remove( node );
        }

        private String randomToken()
        {
            return random.among( TOKENS );
        }

        Label randomLabel()
        {
            return DynamicLabel.label( randomToken() );
        }

        RelationshipType randomRelationshipType()
        {
            return DynamicRelationshipType.withName( randomToken() );
        }

        String randomPropertyKey()
        {
            return randomToken();
        }

        Object randomPropertyValue()
        {
            return random.propertyValue();
        }

        int nodeCount()
        {
            return nodes.size();
        }

        Relationship createRelationship( Node node1, Node node2, RelationshipType type )
        {
            Relationship relationship = node1.createRelationshipTo( node2, type );
            relationships.add( relationship );
            return relationship;
        }
    }
}
