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

package org.neo4j.graphalgo.benchmark.graphgeneration;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;

public class LayeredGraph
{
    List<List<Node>> layers = new LinkedList<List<Node>>();
    NeoService neo;
    RelationshipType relationshipType;
    Random random = new Random( System.currentTimeMillis() );
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

    public Node getRandomNodeFromLayer( int layerIndex )
    {
        List<Node> layer = layers.get( layerIndex );
        return layer.get( random.nextInt( layer.size() ) );
    }

    public LayeredGraph( NeoService neo, RelationshipType relationshipType,
        int numberOfLayers, int numberOfNodesInEachLayer,
        int numberOfEdgesBetweenEachLayer )
    {
        super();
        this.neo = neo;
        this.relationshipType = relationshipType;
        transaction = neo.beginTx();
        for ( int l = 0; l < numberOfLayers; ++l )
        {
            System.out.println( "Creating nodes on layer " + l + " / "
                + numberOfLayers );
            LinkedList<Node> layer = new LinkedList<Node>();
            for ( int n = 0; n < numberOfNodesInEachLayer; ++n )
            {
                layer.add( neo.createNode() );
                renewTransaction();
            }
            layers.add( layer );
        }
        for ( int l = 0; l < numberOfLayers - 1; ++l )
        {
            System.out.println( "Creating edges on layer " + l + " / "
                + numberOfLayers );
            List<Node> layer1 = layers.get( l );
            List<Node> layer2 = layers.get( l + 1 );
            for ( int e = 0; e < numberOfEdgesBetweenEachLayer; ++e )
            {
                Node n1 = layer1.get( random.nextInt( layer1.size() ) );
                Node n2 = layer2.get( random.nextInt( layer2.size() ) );
                Relationship relationship = n1.createRelationshipTo( n2,
                    relationshipType );
                relationship.setProperty( "cost", random.nextDouble() );
                renewTransaction();
            }
        }
        transaction.success();
        transaction.finish();
    }
}
