/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;

public class TestAbandonedIndexEntries extends Neo4jTestCase
{
    @Test
    public void canDeleteIndexEvenIfEntitiesAreFoundToBeAbandonedInTheSameTx()
    {
        // create and index a node
        Index<Node> nodeIndex = graphDb().index().forNodes( "index" );
        Node node = graphDb().createNode();
        nodeIndex.add( node, "key", "value" );
        // make sure to commit the creation of the entry
        restartTx();

        // delete the node to abandon the index entry
        node.delete();
        restartTx();

        // iterate over all nodes indexed with the key to discover abandoned
        for ( @SuppressWarnings( "unused" ) Node hit : nodeIndex.get( "key", "value" ) );

        nodeIndex.delete();
        restartTx();
    }
}
