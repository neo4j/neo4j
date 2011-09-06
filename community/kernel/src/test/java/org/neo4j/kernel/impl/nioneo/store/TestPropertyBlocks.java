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
        // Fill it up, each integer is one block
        Node node = getGraphDb().createNode();
        for ( int i = 0; i < PropertyType.getPayloadSizeLongs(); i++ )
        {
            node.setProperty( "prop" + i, i );
        }

        newTransaction();

        // Remove all but one and add one
        for ( int i = 0; i < PropertyType.getPayloadSizeLongs() - 1; i++ )
        {
            assertEquals( i, node.removeProperty( "prop" + i ) );
        }
        node.setProperty( "profit", 5 );

        newTransaction();

        // Verify
        int remainingProperty = PropertyType.getPayloadSizeLongs() - 1;
        assertEquals( remainingProperty,
                node.getProperty( "prop" + remainingProperty ) );
        assertEquals( 5, node.getProperty( "profit" ) );
    }

    @Test
    public void checkPacking()
    {
        long inUseBefore = propertyRecordsInUse();

        // Fill it up, each integer is one block
        Node node = getGraphDb().createNode();
        node.setProperty( "prop0", 0 );
        newTransaction();

        // One record must have been added
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Since integers take up one block, adding the remaining should not
        // create a new record.
        for ( int i = 1; i < PropertyType.getPayloadSizeLongs(); i++ )
        {
            node.setProperty( "prop" + i, i );
        }
        newTransaction();

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Removing one and adding one of the same size should not create a new
        // record.
        assertEquals( 0, node.removeProperty( "prop0" ) );
        node.setProperty( "prop-1", -1 );
        newTransaction();

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Removing two that take up 1 block and adding one that takes up 2
        // should not create a new record.
        assertEquals( -1, node.removeProperty( "prop-1" ) );
        // Hopefully prop1 exists, meaning payload is at least 16
        assertEquals( 1, node.removeProperty( "prop1" ) );
        // A double value should do the trick
        node.setProperty( "propDouble", 1.0 );
        newTransaction();

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Adding just one now should create a new property record.
        node.setProperty( "prop-2", -2 );
        newTransaction();
        assertEquals( inUseBefore + 2, propertyRecordsInUse() );
    }
}
