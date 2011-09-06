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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestPropertyBlocks extends AbstractNeo4jTestCase
{
    @Test
    public void deleteAndAddToFullPropertyRecord()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "prop1", 1 );
        node.setProperty( "prop2", 2 );
        node.setProperty( "prop3", 3 );
        node.setProperty( "prop4", 4 );

        newTransaction();
        System.out.println( "check1" );

        node.removeProperty( "prop1" );
        node.removeProperty( "prop2" );
        node.removeProperty( "prop3" );
        node.setProperty( "profit", 5 );

        newTransaction();
        System.out.println( "check2" );

        assertEquals( 4, node.getProperty( "prop4" ) );
        assertEquals( 5, node.getProperty( "profit" ) );
    }
}
