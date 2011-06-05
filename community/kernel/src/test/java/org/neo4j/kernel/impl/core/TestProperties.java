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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestProperties extends AbstractNeo4jTestCase
{
    @Test
    public void removeAndAddSameProperty() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "foo", "bar" );
        newTransaction();
        
        node.removeProperty( "foo" );
        node.setProperty( "foo", "bar" );
        newTransaction();
        assertEquals( "bar", node.getProperty( "foo" ) );

        node.setProperty( "foo", "bar" );
        node.removeProperty( "foo" );
        newTransaction();
        assertNull( node.getProperty( "foo", null ) );
    }
    
    @Test
    public void removeSomeAndSetSome() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "remove me", "trash" );
        newTransaction();
        
        node.removeProperty( "remove me" );
        node.setProperty( "foo", "bar" );
        node.setProperty( "baz", 17 );
        newTransaction();
        
        assertEquals( "bar", node.getProperty( "foo" ) );
        assertEquals( 17, node.getProperty( "baz" ) );
        assertNull( node.getProperty( "remove me", null ) );
    }
    
    @Test
    public void removeOneOfThree() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "1", 1 );
        node.setProperty( "2", 2 );
        node.setProperty( "3", 3 );
        newTransaction();
        
        node.removeProperty( "2" );
        newTransaction();
        assertNull( node.getProperty( "2", null ) );
    }
}
