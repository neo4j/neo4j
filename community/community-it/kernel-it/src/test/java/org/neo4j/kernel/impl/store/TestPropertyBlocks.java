/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.store;

import org.eclipse.collections.api.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TestPropertyBlocks extends AbstractNeo4jTestCase
{
    @AfterEach
    void tearDown()
    {
        stopDb();
        startDb();
    }

    @Test
    void simpleAddIntegers()
    {
        long inUseBefore = propertyRecordsInUse();
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs(); i++ )
            {
                node.setProperty( "prop" + i, i );
                assertEquals( i, node.getProperty( "prop" + i ) );
            }
            transaction.commit();
        }
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs(); i++ )
            {
                assertEquals( i, node.getProperty( "prop" + i ) );
            }

            for ( int i = 0; i < PropertyType.getPayloadSizeLongs(); i++ )
            {
                assertEquals( i, node.removeProperty( "prop" + i ) );
                assertFalse( node.hasProperty( "prop" + i ) );
            }
            transaction.commit();
        }
        assertEquals( inUseBefore, propertyRecordsInUse() );
    }

    @Test
    void simpleAddDoubles()
    {
        long inUseBefore = propertyRecordsInUse();
        Node node = createNode();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++ )
            {
                node.setProperty( "prop" + i, i * -1.0 );
                assertEquals( i * -1.0, node.getProperty( "prop" + i ) );
            }
            transaction.commit();
        }

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++ )
            {
                assertEquals( i * -1.0, node.getProperty( "prop" + i ) );
            }

            for ( int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++ )
            {
                assertEquals( i * -1.0, node.removeProperty( "prop" + i ) );
                assertFalse( node.hasProperty( "prop" + i ) );
            }
            transaction.commit();
        }
        assertEquals( inUseBefore, propertyRecordsInUse() );
    }

    @Test
    void deleteEverythingInMiddleRecord()
    {
        long inUseBefore = propertyRecordsInUse();
        Node node = createNode();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < 3 * PropertyType.getPayloadSizeLongs(); i++ )
            {
                node.setProperty( "shortString" + i, String.valueOf( i ) );
            }
            transaction.commit();
        }
        assertEquals( inUseBefore + 3, propertyRecordsInUse() );

        final List<Pair<String, Object>> middleRecordProps = getPropertiesFromRecord( 1 );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            middleRecordProps.forEach( nameAndValue ->
            {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals( value, node.removeProperty( name ) );
            } );
            transaction.commit();
        }

        assertEquals( inUseBefore + 2, propertyRecordsInUse() );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            middleRecordProps.forEach( nameAndValue -> assertFalse( node.hasProperty( nameAndValue.getOne() ) ) );
            getPropertiesFromRecord( 0 ).forEach( nameAndValue ->
            {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals( value, node.removeProperty( name ) );
            } );
            getPropertiesFromRecord( 2 ).forEach( nameAndValue ->
            {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals( value, node.removeProperty( name ) );
            } );
            transaction.commit();
        }
    }

    private List<Pair<String, Object>> getPropertiesFromRecord( long recordId )
    {
        final List<Pair<String, Object>> props = new ArrayList<>();
        final PropertyRecord record = propertyStore().getRecord( recordId, propertyStore().newRecord(), RecordLoad.FORCE );
        record.forEach( block ->
        {
            final Object value = propertyStore().getValue( block ).asObject();
            final String name = propertyStore().getPropertyKeyTokenStore().getToken( block.getKeyIndexId() ).name();
            props.add( pair( name, value ) );
        } );
        return props;
    }

    @Test
    void largeTx()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node = getGraphDb().createNode();

            node.setProperty( "anchor", "hi" );
            for ( int i = 0; i < 255; i++ )
            {
                node.setProperty( "foo", 1 );
                node.removeProperty( "foo" );
            }
            transaction.commit();
        }
    }

    /*
     * Creates a PropertyRecord, fills it up, removes something and
     * adds something that should fit.
     */
    @Test
    void deleteAndAddToFullPropertyRecord()
    {
        // Fill it up, each integer is one block
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs(); i++ )
            {
                node.setProperty( "prop" + i, i );
            }
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // Remove all but one and add one
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs() - 1; i++ )
            {
                assertEquals( i, node.removeProperty( "prop" + i ) );
            }
            node.setProperty( "profit", 5 );
            transaction.commit();
        }

        // Verify
        int remainingProperty = PropertyType.getPayloadSizeLongs() - 1;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( remainingProperty, node.getProperty( "prop" + remainingProperty ) );
            assertEquals( 5, node.getProperty( "profit" ) );
            transaction.commit();
        }
    }

    @Test
    void checkPacking()
    {
        long inUseBefore = propertyRecordsInUse();

        // Fill it up, each integer is one block
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "prop0", 0 );
            transaction.commit();
        }

        // One record must have been added
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // Since integers take up one block, adding the remaining should not
            // create a new record.
            for ( int i = 1; i < PropertyType.getPayloadSizeLongs(); i++ )
            {
                node.setProperty( "prop" + i, i );
            }
            transaction.commit();
        }

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // Removing one and adding one of the same size should not create a new
            // record.
            assertEquals( 0, node.removeProperty( "prop0" ) );
            node.setProperty( "prop-1", -1 );
            transaction.commit();
        }

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // Removing two that take up 1 block and adding one that takes up 2
            // should not create a new record.
            assertEquals( -1, node.removeProperty( "prop-1" ) );
            // Hopefully prop1 exists, meaning payload is at least 16
            assertEquals( 1, node.removeProperty( "prop1" ) );
            // A double value should do the trick
            node.setProperty( "propDouble", 1.0 );
            transaction.commit();
        }

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Adding just one now should create a new property record.
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "prop-2", -2 );
            transaction.commit();
        }
        assertEquals( inUseBefore + 2, propertyRecordsInUse() );
    }

    @Test
    void substituteOneLargeWithManySmallPropBlocks()
    {
        Node node = createNode();
        long inUseBefore = propertyRecordsInUse();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            /*
             * Fill up with doubles and the rest with ints - we assume
             * the former take up two blocks, the latter 1.
             */
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs() / 2; i++ )
            {
                node.setProperty( "double" + i, i * 1.0 );
            }
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            /*
             * I know this is stupid in that it is executed 0 or 1 times but it
             * is easier to maintain and change for different payload sizes.
             */
            for ( int i = 0; i < PropertyType.getPayloadSizeLongs() % 2; i++ )
            {
                node.setProperty( "int" + i, i );
            }
            transaction.commit();
        }

        // Just checking that the assumptions above is correct
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // We assume at least one double has been added
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.removeProperty( "double0" );
            transaction.commit();
        }
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // Do the actual substitution, check that no record is created
            node.setProperty( "int-1", -1 );
            node.setProperty( "int-2", -2 );
            transaction.commit();
        }
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        // Finally, make sure we actually are with a full prop record
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "int-3", -3 );
            transaction.commit();
        }
        assertEquals( inUseBefore + 2, propertyRecordsInUse() );
    }

    /*
     * Adds at least 3 1-block properties and removes the first and third.
     * Adds a 2-block property and checks if it is added in the same record.
     */
    @Test
    void testBlockDefragmentationWithTwoSpaces()
    {
        assumeTrue( PropertyType.getPayloadSizeLongs() > 2 );
        Node node = createNode();
        long inUseBefore = propertyRecordsInUse();

        int stuffedIntegers = 0;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( ; stuffedIntegers < PropertyType.getPayloadSizeLongs(); stuffedIntegers++ )
            {
                node.setProperty( "int" + stuffedIntegers, stuffedIntegers );
            }
            transaction.commit();
        }

        // Basic check that integers take up one (8 byte) block.
        assertEquals( stuffedIntegers, PropertyType.getPayloadSizeLongs() );

        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // Remove first and third
            node.removeProperty( "int0" );
            node.removeProperty( "int2" );
            transaction.commit();
        }
        // Add the two block thing.
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "theDouble", 1.0 );
            transaction.commit();
        }
        // Let's make sure everything is in one record and with proper values.
        assertEquals( inUseBefore + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertNull( node.getProperty( "int0", null ) );
            assertEquals( 1, node.getProperty( "int1" ) );
            assertNull( node.getProperty( "int2", null ) );
            for ( int i = 3; i < stuffedIntegers; i++ )
            {
                assertEquals( i, node.getProperty( "int" + i ) );
            }
            assertEquals( 1.0, node.getProperty( "theDouble" ) );
            transaction.commit();
        }
    }

    @Test
    void checkDeletesRemoveRecordsWhenProper()
    {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedBooleans = 0;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( ; stuffedBooleans < PropertyType.getPayloadSizeLongs(); stuffedBooleans++ )
            {
                node.setProperty( "boolean" + stuffedBooleans, stuffedBooleans % 2 == 0 );
            }
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "theExraOne", true );
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < stuffedBooleans; i++ )
            {
                assertEquals( Boolean.valueOf( i % 2 == 0 ), node.removeProperty( "boolean" + i ) );
            }
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < stuffedBooleans; i++ )
            {
                assertFalse( node.hasProperty( "boolean" + i ) );
            }
            assertEquals( Boolean.TRUE, node.getProperty( "theExraOne" ) );
            transaction.commit();
        }
    }

    /*
     * Creates 3 records and deletes stuff from the middle one. Assumes that a 2 character
     * string that is a number fits in one block.
     */
    @Test
    void testMessWithMiddleRecordDeletes()
    {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedShortStrings = 0;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( ; stuffedShortStrings < 3 * PropertyType.getPayloadSizeLongs(); stuffedShortStrings++ )
            {
                node.setProperty( "shortString" + stuffedShortStrings, String.valueOf( stuffedShortStrings ) );
            }
            transaction.commit();
        }
        assertEquals( recordsInUseAtStart + 3, propertyRecordsInUse() );

        final List<Pair<String, Object>> middleRecordProps = getPropertiesFromRecord( 1 );
        final Pair<String, Object> secondBlockInMiddleRecord = middleRecordProps.get( 1 );
        final Pair<String, Object> thirdBlockInMiddleRecord = middleRecordProps.get( 2 );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( secondBlockInMiddleRecord.getTwo(), node.removeProperty( secondBlockInMiddleRecord.getOne() ) );
            assertEquals( thirdBlockInMiddleRecord.getTwo(), node.removeProperty( thirdBlockInMiddleRecord.getOne() ) );
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 3, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < stuffedShortStrings; i++ )
            {
                if ( secondBlockInMiddleRecord.getTwo().equals( String.valueOf( i ) ) || thirdBlockInMiddleRecord.getTwo().equals( String.valueOf( i ) ) )
                {
                    assertFalse( node.hasProperty( "shortString" + i ) );
                }
                else
                {
                    assertEquals( String.valueOf( i ), node.getProperty( "shortString" + i ) );
                }
            }
            // Start deleting stuff. First, all the middle property blocks
            int deletedProps = 0;

            for ( Pair<String,Object> prop : middleRecordProps )
            {
                final String name = prop.getOne();
                if ( node.hasProperty( name ) )
                {
                    deletedProps++;
                    node.removeProperty( name );
                }
            }

            assertEquals( PropertyType.getPayloadSizeLongs() - 2, deletedProps );
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            middleRecordProps.forEach( nameAndValue -> assertFalse( node.hasProperty( nameAndValue.getOne() ) ) );
            getPropertiesFromRecord( 0 ).forEach( nameAndValue ->
            {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals( value, node.removeProperty( name ) );
            } );
            getPropertiesFromRecord( 2 ).forEach( nameAndValue ->
            {
                final String name = nameAndValue.getOne();
                final Object value = nameAndValue.getTwo();
                assertEquals( value, node.removeProperty( name ) );
            } );
            transaction.commit();
        }
    }

    @Test
    void mixAndPackDifferentTypes()
    {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        int stuffedShortStrings = 0;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( ; stuffedShortStrings < PropertyType.getPayloadSizeLongs(); stuffedShortStrings++ )
            {
                node.setProperty( "shortString" + stuffedShortStrings, String.valueOf( stuffedShortStrings ) );
            }
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.removeProperty( "shortString0" );
            node.removeProperty( "shortString2" );
            node.setProperty( "theDoubleOne", -1.0 );
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
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
                    assertEquals( String.valueOf( i ), node.getProperty( "shortString" + i ) );
                }
            }
            assertEquals( -1.0, node.getProperty( "theDoubleOne" ) );
            transaction.commit();
        }
    }

    @Test
    void testAdditionsHappenAtTheFirstRecordIfFits1()
    {
        Node node = createNode();
        long recordsInUseAtStart = propertyRecordsInUse();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "int1", 1 );
            node.setProperty( "double1", 1.0 );
            node.setProperty( "int2", 2 );
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.removeProperty( "double1" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "double2", 1.0 );
            transaction.commit();
        }
        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "paddingBoolean", false );
            transaction.commit();
        }
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );
    }

    @Test
    void testAdditionHappensInTheMiddleIfItFits()
    {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "int1", 1 );
            node.setProperty( "double1", 1.0 );
            node.setProperty( "int2", 2 );

            int stuffedShortStrings = 0;
            for ( ; stuffedShortStrings < PropertyType.getPayloadSizeLongs(); stuffedShortStrings++ )
            {
                node.setProperty( "shortString" + stuffedShortStrings, String.valueOf( stuffedShortStrings ) );
            }
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.removeProperty( "shortString" + 1 );
            node.setProperty( "int3", 3 );
            transaction.commit();
        }
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );
    }

    @Test
    void testChangePropertyType()
    {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            int stuffedShortStrings = 0;
            for ( ; stuffedShortStrings < PropertyType.getPayloadSizeLongs(); stuffedShortStrings++ )
            {
                node.setProperty( "shortString" + stuffedShortStrings, String.valueOf( stuffedShortStrings ) );
            }
            transaction.commit();
        }
        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "shortString1", 1.0 );
            transaction.commit();
        }
    }

    @Test
    void testRevertOverflowingChange()
    {
        long recordsInUseAtStart;
        Relationship rel;
        long valueRecordsInUseAtStart;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            rel = getGraphDb().createNode().createRelationshipTo( getGraphDb().createNode(), RelationshipType.withName( "INVALIDATES" ) );

            recordsInUseAtStart = propertyRecordsInUse();
            valueRecordsInUseAtStart = dynamicArrayRecordsInUse();

            rel.setProperty( "theByte", (byte) -8 );
            rel.setProperty( "theDoubleThatGrows", Math.PI );
            rel.setProperty( "theInteger", -444345 );

            rel.setProperty( "theDoubleThatGrows", new long[]{1L << 63, 1L << 63, 1L << 63} );

            rel.setProperty( "theDoubleThatGrows", Math.E );
            transaction.commit();
        }

        // Then
        /*
         * The following line should pass if we have packing on property block
         * size shrinking.
         */
        // assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
            assertEquals( valueRecordsInUseAtStart, dynamicArrayRecordsInUse() );

            assertEquals( (byte) -8, rel.getProperty( "theByte" ) );
            assertEquals( -444345, rel.getProperty( "theInteger" ) );
            assertEquals( Math.E, rel.getProperty( "theDoubleThatGrows" ) );
            transaction.commit();
        }
    }

    @Test
    void testYoYoArrayPropertyWithinTx()
    {
        testArrayBase( false );
    }

    @Test
    void testYoYoArrayPropertyOverTxs()
    {
        testArrayBase( true );
    }

    private void testArrayBase( boolean withNewTx )
    {
        Relationship rel;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            rel = getGraphDb().createNode().createRelationshipTo( getGraphDb().createNode(), RelationshipType.withName( "LOCKS" ) );
            transaction.commit();
        }

        long recordsInUseAtStart = propertyRecordsInUse();
        long valueRecordsInUseAtStart = dynamicArrayRecordsInUse();

        List<Long> theData = new ArrayList<>();
        Transaction tx = getGraphDb().beginTx();
        for ( int i = 0; i < PropertyType.getPayloadSizeLongs() - 1; i++ )
        {
            theData.add( 1L << 63 );
            Long[] value = theData.toArray( new Long[] {} );
            rel.setProperty( "yoyo", value );
            if ( withNewTx )
            {
                tx.commit();
                tx = getGraphDb().beginTx();
                assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
                assertEquals( valueRecordsInUseAtStart, dynamicArrayRecordsInUse() );
            }
        }
        tx.commit();

        theData.add( 1L << 63 );
        Long[] value = theData.toArray( new Long[] {} );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            rel.setProperty( "yoyo", value );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
            assertEquals( valueRecordsInUseAtStart + 1, dynamicArrayRecordsInUse() );
            rel.setProperty( "filler", new long[]{1L << 63, 1L << 63, 1L << 63} );
            transaction.commit();
        }
        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );
    }

    @Test
    void testRemoveZigZag()
    {
        long recordsInUseAtStart;
        int propRecCount = 1;
        Relationship rel;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            rel = getGraphDb().createNode().createRelationshipTo( getGraphDb().createNode(), RelationshipType.withName( "LOCKS" ) );

            recordsInUseAtStart = propertyRecordsInUse();

            for ( ; propRecCount <= 3; propRecCount++ )
            {
                for ( int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++ )
                {
                    rel.setProperty( "int" + (propRecCount * 10 + i), propRecCount * 10 + i );
                }
            }
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 3, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++ )
            {
                for ( int j = 1; j < propRecCount; j++ )
                {
                    assertEquals( j * 10 + i, rel.removeProperty( "int" + (j * 10 + i) ) );
                    if ( i == PropertyType.getPayloadSize() - 1 && j != propRecCount - 1 )
                    {
                        assertEquals( recordsInUseAtStart + (propRecCount - j), propertyRecordsInUse() );
                    }
                    else if ( i == PropertyType.getPayloadSize() - 1 && j == propRecCount - 1 )
                    {
                        assertEquals( recordsInUseAtStart, propertyRecordsInUse() );
                    }
                    else
                    {
                        assertEquals( recordsInUseAtStart + 3, propertyRecordsInUse() );
                    }
                }
            }
            for ( int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++ )
            {
                for ( int j = 1; j < propRecCount; j++ )
                {
                    assertFalse( rel.hasProperty( "int" + (j * 10 + i) ) );
                }
            }
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 1; i <= PropertyType.getPayloadSizeLongs(); i++ )
            {
                for ( int j = 1; j < propRecCount; j++ )
                {
                    assertFalse( rel.hasProperty( "int" + (j * 10 + i) ) );
                }
            }
            transaction.commit();
        }
        assertEquals( recordsInUseAtStart, propertyRecordsInUse() );
    }

    @Test
    void testSetWithSameValue()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "rev_pos", "40000633e7ad67ff" );
            assertEquals( "40000633e7ad67ff", node.getProperty( "rev_pos" ) );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "rev_pos", "40000633e7ad67ef" );
            assertEquals( "40000633e7ad67ef", node.getProperty( "rev_pos" ) );
            transaction.commit();
        }
    }

    private void testStringBase( boolean withNewTx )
    {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();
        long valueRecordsInUseAtStart = dynamicStringRecordsInUse();

        String data = "0";
        int counter = 1;
        Transaction tx = getGraphDb().beginTx();
        while ( dynamicStringRecordsInUse() == valueRecordsInUseAtStart )
        {
            data += counter++;
            node.setProperty( "yoyo", data );
            if ( withNewTx )
            {
                tx.commit();
                tx = getGraphDb().beginTx();
                assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );
            }
        }
        tx.commit();

        data = data.substring( 0, data.length() - 2 );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "yoyo", data );
            transaction.commit();
        }

        assertEquals( valueRecordsInUseAtStart, dynamicStringRecordsInUse() );
        assertEquals( recordsInUseAtStart + 1, propertyRecordsInUse() );

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "fillerBoolean", true );
            transaction.commit();
        }

        assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );
    }

    @Test
    void testStringWithTx()
    {
        testStringBase( true );
    }

    @Test
    void testRemoveFirstOfTwo()
    {
        Node node = createNode();

        long recordsInUseAtStart = propertyRecordsInUse();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "Double1", 1.0 );
            node.setProperty( "Int1", 1 );
            node.setProperty( "Int2", 2 );
            node.setProperty( "Int2", 1.2 );
            node.setProperty( "Int2", 2 );
            node.setProperty( "Double3", 3.0 );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( recordsInUseAtStart + 2, propertyRecordsInUse() );
            assertEquals( 1.0, node.getProperty( "Double1" ) );
            assertEquals( 1, node.getProperty( "Int1" ) );
            assertEquals( 2, node.getProperty( "Int2" ) );
            assertEquals( 3.0, node.getProperty( "Double3" ) );
            transaction.commit();
        }
    }

    @Test
    void deleteNodeWithNewPropertyRecordShouldFreeTheNewRecord()
    {
        final long propcount = getIdGenerator( IdType.PROPERTY ).getNumberOfIdsInUse();
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "one", 1 );
            node.setProperty( "two", 2 );
            node.setProperty( "three", 3 );
            node.setProperty( "four", 4 );
            transaction.commit();
        }
        assertEquals( propcount + 1, propertyRecordsInUse(), "Invalid assumption: property record count" );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "final", 666 );
            transaction.commit();
        }
        assertEquals( propcount + 2, propertyRecordsInUse(), "Invalid assumption: property record count" );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.delete();
            transaction.commit();
        }
        assertEquals( propcount, propertyRecordsInUse(), "All property records should be freed" );
    }
}
