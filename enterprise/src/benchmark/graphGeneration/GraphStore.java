/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.benchmark.graphGeneration;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Transaction;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;

public class GraphStore
{
    NeoService neo;

    protected static enum MyRelTypes implements RelationshipType
    {
        STOREDGRAPH, NODEGROUP, HASNODE
    }
    public class Graph
    {
        Node referenceNode;
        Transaction transaction;
        int transactionCount = 0;

        protected void renewTransaction()
        {
            if ( ++transactionCount > 1000 )
            {
                transactionCount = 0;
                transaction.success();
                transaction.finish();
                transaction = neo.beginTx();
            }
        }

        public Graph()
        {
            Transaction transaction = neo.beginTx();
            this.referenceNode = neo.createNode();
            transaction.success();
            transaction.finish();
        }

        public Graph( Node referenceNode )
        {
            super();
            this.referenceNode = referenceNode;
        }

        public long[] loadNodeIds()
        {
            return loadNodeIds( "" );
        }

        public void saveNodeIds( long[] nodeIds )
        {
            saveNodeIds( nodeIds, "" );
        }

        public void saveNodeIds( long[] nodeIds, String NodeGroup )
        {
            transaction = neo.beginTx();
            Node groupNode = neo.createNode();
            Relationship relationship = referenceNode.createRelationshipTo(
                groupNode, MyRelTypes.NODEGROUP );
            relationship.setProperty( "NodeGroup", NodeGroup );
            groupNode.setProperty( "numberOfNodes", (Integer) nodeIds.length );
            for ( int n = 0; n < nodeIds.length; ++n )
            {
                Node node = neo.getNodeById( nodeIds[n] );
                groupNode.createRelationshipTo( node, MyRelTypes.HASNODE );
                renewTransaction();
            }
            transaction.success();
            transaction.finish();
        }

        public long[] loadNodeIds( String NodeGroup )
        {
            transaction = neo.beginTx();
            Traverser traverser = referenceNode.traverse( Order.BREADTH_FIRST,
                StopEvaluator.DEPTH_ONE,
                ReturnableEvaluator.ALL_BUT_START_NODE, MyRelTypes.NODEGROUP,
                Direction.BOTH );
            Node parentNode = null;
            for ( Node node : traverser )
            {
                if ( traverser.currentPosition().lastRelationshipTraversed()
                    .getProperty( "NodeGroup" ).equals( NodeGroup ) )
                {
                    parentNode = node;
                    break;
                }
            }
            long[] nodes = null;
            if ( parentNode != null )
            {
                Integer numberOfNodes = (Integer) parentNode
                    .getProperty( "numberOfNodes" );
                nodes = new long[numberOfNodes];
                traverser = parentNode.traverse( Order.BREADTH_FIRST,
                    StopEvaluator.DEPTH_ONE,
                    ReturnableEvaluator.ALL_BUT_START_NODE, MyRelTypes.HASNODE,
                    Direction.BOTH );
                int n = 0;
                for ( Node node : traverser )
                {
                    nodes[n++] = node.getId();
                    if ( n > numberOfNodes )
                    {
                        // Loading too many, bail
                        nodes = null;
                        break;
                    }
                }
            }
            transaction.success();
            transaction.finish();
            return nodes;
        }

        public Node getReferenceNode()
        {
            return referenceNode;
        }
    }

    public Graph loadGraph( String identifier )
    {
        Transaction transaction = neo.beginTx();
        Traverser traverser = neo.getReferenceNode().traverse(
            Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE,
            ReturnableEvaluator.ALL_BUT_START_NODE, MyRelTypes.STOREDGRAPH,
            Direction.BOTH );
        Node result = null;
        for ( Node node : traverser )
        {
            if ( !node.hasProperty( "GraphIdentifier" ) )
            {
                continue;
            }
            if ( node.getProperty( "GraphIdentifier" ).equals( identifier ) )
            {
                result = node;
                break;
            }
        }
        transaction.success();
        transaction.finish();
        if ( result == null )
        {
            return null;
        }
        return new Graph( result );
    }

    public Graph createGraph(String identifier )
    {
        Transaction transaction = neo.beginTx();
        Graph graph = new Graph();
        neo.getReferenceNode().createRelationshipTo( graph.getReferenceNode(), MyRelTypes.STOREDGRAPH );
        graph.getReferenceNode().setProperty( "GraphIdentifier", identifier );
        transaction.success();
        transaction.finish();
        return graph;
    }

    public GraphStore( NeoService neo )
    {
        super();
        this.neo = neo;
    }
}
