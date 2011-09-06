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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.junit.Assume;
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

    @Test
    public void substituteOneLargeWithManySmallPropBlocks()
    {
        Node node = getGraphDb().createNode();
        long inUseBefore = propertyRecordsInUse();
        /*
         * Fill up with doubles and the rest with ints - we assume
         * the former take up two blocks, the latter 1.
         */
        for ( int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++ )
        {
            node.setProperty( "double" + i, i * 1.0 );
        }
        /*
         * I know this is stupid in that it is executed 0 or 1 times but it
         * is easier to maintain and change for different payload sizes.
         */
        for ( int i = 0; i < PropertyType.getPayloadSizeLongs() % 2; i++ )
        {
            node.setProperty( "int" + i, i );
        }
        newTransaction();

        // Just checking that the assumptions above is correct
        assertEquals(inUseBefore+1, propertyRecordsInUse());

        // We assume at least one double has been added
        node.removeProperty( "double0" );
        newTransaction();
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Do the actual substitution, check that no record is created
        node.setProperty( "int-1", -1 );
        node.setProperty( "int-2", -2 );
        newTransaction();
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Finally, make sure we actually are with a full prop record
        node.setProperty( "int-3", -3 );
        newTransaction();
        assertEquals( inUseBefore + 2, propertyRecordsInUse() );
    }

    /*
     * Adds at least 3 1-block properties and removes the first and third.
     * Adds a 2-block property and checks if it is added in the same record.
     */
    @Test
    public void testBlockDefragmentationWithTwoSpaces()
    {
        Assume.assumeTrue( PropertyType.getPayloadSizeLongs() > 2 );
        Node node = getGraphDb().createNode();
        long inUseBefore = propertyRecordsInUse();

        int stuffedIntegers = 0;
        for ( ; stuffedIntegers < PropertyType.getPayloadSizeLongs(); stuffedIntegers++ )
        {
            node.setProperty( "int" + stuffedIntegers, stuffedIntegers );
        }

        // Basic check that integers take up one (8 byte) block.
        assertEquals( stuffedIntegers, PropertyType.getPayloadSizeLongs() );
        newTransaction();

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Remove first and third
        node.removeProperty( "int0" );
        node.removeProperty( "int2" );
        newTransaction();
        // Add the two block thing.
        node.setProperty( "theDouble", 1.0 );
        newTransaction();

        // Let's make sure everything is in one record and with proper values.
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        assertNull( node.getProperty( "int0", null ) );
        assertEquals( 1, node.getProperty( "int1" ) );
        assertNull( node.getProperty( "int2", null ) );
        for ( int i = 3; i < stuffedIntegers; i++ )
        {
            assertEquals( i, node.getProperty( "int" + i ) );
        }
        assertEquals( 1.0, node.getProperty( "theDouble" ) );
    }

    @Test
    public void checkDeletesRemoveRecordsWhenProper()
    {
        Node node = getGraphDb().createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedBooleans = 0;
        for ( ; stuffedBooleans < PropertyType.getPayloadSizeLongs(); stuffedBooleans++ )
        {
            node.setProperty( "boolean" + stuffedBooleans,
                    stuffedBooleans % 2 == 0 );
        }

        newTransaction();

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        node.setProperty( "theExraOne", true );

        newTransaction();

        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );

        for ( int i = 0; i < stuffedBooleans; i++ )
        {
            assertEquals( Boolean.valueOf( i % 2 == 0 ),
                    node.removeProperty( "boolean" + i ) );
        }

        newTransaction();

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        for ( int i = 0; i < stuffedBooleans; i++ )
        {
            assertFalse( node.hasProperty( "boolean" + i ) );
        }
        assertEquals( Boolean.TRUE, node.getProperty( "theExraOne" ) );
    }
}
