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

    /*
     * Creates 3 records and deletes stuff from the middle one. Assumes that a 2 character
     * string that is a number fits in one block.
     */
    @Test
    public void testMessWithMiddleRecordDeletes()
    {
        Node node = getGraphDb().createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedShortStrings = 0;
        for ( ; stuffedShortStrings < 3 * PropertyType.getPayloadSizeLongs(); stuffedShortStrings++ )
        {
            node.setProperty( "shortString" + stuffedShortStrings,
                    String.valueOf( stuffedShortStrings ) );
        }
        newTransaction();
        assertEquals( recordsInUseAtStart + 3, propertyRecordsInUse() );

        int secondBlockInSecondRecord = PropertyType.getPayloadSizeLongs() + 1;
        int thirdBlockInSecondRecord = PropertyType.getPayloadSizeLongs() + 2;

        assertEquals( String.valueOf( secondBlockInSecondRecord ),
                node.removeProperty( "shortString" + secondBlockInSecondRecord ) );
        assertEquals( String.valueOf( thirdBlockInSecondRecord ),
                node.removeProperty( "shortString" + thirdBlockInSecondRecord ) );

        newTransaction();
        assertEquals( recordsInUseAtStart + 3, propertyRecordsInUse() );

        for ( int i = 0; i < stuffedShortStrings; i++ )
        {
            if ( i == secondBlockInSecondRecord )
            {
                assertFalse( node.hasProperty( "shortString" + i ) );
            }
            else if ( i == thirdBlockInSecondRecord )
            {
                assertFalse( node.hasProperty( "shortString" + i ) );
            }
            else
            {
                assertEquals( String.valueOf( i ),
                        node.getProperty( "shortString" + i ) );
            }
        }
        // Start deleting stuff. First, all the middle property blocks
        int deletedProps = 0;
        for ( int i = PropertyType.getPayloadSizeLongs(); i < PropertyType.getPayloadSizeLongs() * 2; i++ )
        {
            if ( node.hasProperty( "shortString" + i ) )
            {
                deletedProps++;
                node.removeProperty( "shortString" + i );
            }
        }
        assertEquals( PropertyType.getPayloadSizeLongs() - 2, deletedProps );

        newTransaction();
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );

        for ( int i = 0; i < PropertyType.getPayloadSizeLongs(); i++ )
        {
                assertEquals( String.valueOf( i ),
                        node.removeProperty( "shortString" + i ) );
        }
        for ( int i = PropertyType.getPayloadSizeLongs(); i < PropertyType.getPayloadSizeLongs() * 2; i++ )
        {
            assertFalse( node.hasProperty( "shortString" + i ) );
        }
        for ( int i = PropertyType.getPayloadSizeLongs() * 2; i < PropertyType.getPayloadSizeLongs() * 3; i++ )
        {
            assertEquals( String.valueOf( i ),
                    node.removeProperty( "shortString" + i ) );
        }
    }

    @Test
    public void mixAndPackDifferentTypes()
    {
        Node node = getGraphDb().createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedShortStrings = 0;
        for ( ; stuffedShortStrings < PropertyType.getPayloadSizeLongs(); stuffedShortStrings++ )
        {
            node.setProperty( "shortString" + stuffedShortStrings,
                    String.valueOf( stuffedShortStrings ) );
        }
        newTransaction();

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        node.removeProperty( "shortString0" );
        node.removeProperty( "shortString2" );
        node.setProperty( "theDoubleOne", -1.0 );
        newTransaction();

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
        for ( int i = 0; i < stuffedShortStrings; i++ )
        {
            if ( i == 0 )
            {
                assertFalse( node.hasProperty( "shortString" + i ) );
            }
            else if ( i == 2 )
            {
                assertFalse( node.hasProperty( "shortString" + i ) );
            }
            else
            {
                assertEquals( String.valueOf( i ),
                        node.getProperty( "shortString" + i ) );
            }
        }
        assertEquals( -1.0, node.getProperty( "theDoubleOne" ) );
    }

    @Test
    public void testAdditionsHappenAtTheFirstRecordIfFits1()
    {
        Node node = getGraphDb().createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        node.setProperty( "int1", 1 );
        node.setProperty("double1", 1.0);
        node.setProperty( "int2", 2 );
        newTransaction();

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        node.removeProperty( "double1" );
        newTransaction();
        node.setProperty( "double2", 1.0 );
        newTransaction();
        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        node.setProperty( "paddingBoolean", false );
        newTransaction();
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );
    }

    @Test
    public void testAdditionsHappenAtTheFirstRecordWhenFits()
    {
        Node node = getGraphDb().createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        node.setProperty( "int1", 1 );
        node.setProperty( "double1", 1.0 );
        node.setProperty( "int2", 2 );
        newTransaction();

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        node.removeProperty( "int1" );
        newTransaction();
        node.setProperty( "double2", 1.0 );
        newTransaction();
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );

        node.removeProperty( "int2" );
        newTransaction();
        node.setProperty( "double3", 1.0 );
        newTransaction();
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );

        node.setProperty( "paddingBoolean", false );
        newTransaction();
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );
    }
}
