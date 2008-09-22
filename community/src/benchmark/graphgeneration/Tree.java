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
package org.neo4j.graphalgo.benchmark.graphgeneration;

import java.util.Random;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;

public class Tree
{
    private long[] nodes;
    private final NeoService neo;
    private final Random random;
    Transaction transaction;
    int transactionCount = 0;
    RelationshipType relationshipType;

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

    public Node getRandomNode( Node butNotThisOne )
    {
        while ( true )
        {
            long id = nodes[random.nextInt( nodes.length )];
            if ( butNotThisOne != null && id == butNotThisOne.getId() )
            {
                continue;
            }
            return neo.getNodeById( id );
        }
    }

    public Node getRootNode()
    {
        return neo.getNodeById( nodes[0] );
    }

    public Tree( final NeoService neo, RelationshipType relationshipType,
        int numberOfNodes )
    {
        super();
        this.neo = neo;
        this.relationshipType = relationshipType;
        random = new Random( System.currentTimeMillis() );
        transaction = neo.beginTx();
        nodes = new long[numberOfNodes];
        // Create a number of nodes, linking each with a random node of the
        // previuos ones.
        nodes[0] = neo.createNode().getId();
        for ( int n = 1; n < numberOfNodes; ++n )
        {
            Node node = neo.createNode();
            nodes[n] = node.getId();
            Node othernode = neo.getNodeById( nodes[random.nextInt( n )] );
            othernode.createRelationshipTo( node, relationshipType );
            renewTransaction();
        }
    }
}
