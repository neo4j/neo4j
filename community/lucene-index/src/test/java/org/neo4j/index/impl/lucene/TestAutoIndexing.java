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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestAutoIndexing extends AbstractNeo4jTestCase
{
    @Test
    public void testAutoIndex()
    {
        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        autoIndexer.addAutoIndexingForNodeProperty( "test_uuid" );
        newTransaction();
        
        Node node1 = getGraphDb().createNode();
        node1.setProperty( "test_uuid", "node1" );
        Node node2 = getGraphDb().createNode();
        node2.setProperty( "test_uuid", "node2" );

        // will index on commit
        assertTrue( !autoIndexer.getNodesFor( "test_uuid", "node1" ).hasNext() );
        assertTrue( !autoIndexer.getNodesFor( "test_uuid", "node2" ).hasNext() );
        
        newTransaction();
        
        assertEquals( node1, autoIndexer.getNodesFor( "test_uuid", "node1" ).getSingle() );
        assertEquals( node2, autoIndexer.getNodesFor( "test_uuid", "node2" ).getSingle() );
    }
}