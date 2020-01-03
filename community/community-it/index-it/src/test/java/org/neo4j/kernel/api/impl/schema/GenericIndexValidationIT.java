/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_BOOLEAN;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_DATE;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_DURATION;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_GEOMETRY;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_LOCAL_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_LOCAL_TIME;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_NUMBER_BYTE;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_NUMBER_DOUBLE;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_NUMBER_FLOAT;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_NUMBER_INT;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_NUMBER_LONG;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_NUMBER_SHORT;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_STRING_LENGTH;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_ZONED_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_ZONED_TIME;
import static org.neo4j.test.TestLabels.LABEL_ONE;

public class GenericIndexValidationIT
{
    private static final String[] PROP_KEYS = new String[]{
            "prop0",
            "prop1",
            "prop2",
            "prop3",
            "prop4"
    };
    private static final int KEY_SIZE_LIMIT = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE );
    private static final int ESTIMATED_OVERHEAD_PER_SLOT = 2;
    private static final int WIGGLE_ROOM = 50;

    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule().withSetting( default_schema_provider, NATIVE_BTREE10.providerName() );

    @ClassRule
    public static RandomRule random = new RandomRule();

    /**
     * Key size validation test for single type.
     *
     * Validate that we handle index reads and writes correctly for dynamically sized values (arrays and strings)
     * of all different types with length close to and over the max limit for given type.
     *
     * We do this by inserting arrays of increasing size (doubling each iteration) and when we hit the upper limit
     * we do binary search between the established min and max limit.
     * We also verify that the largest successful array length for each type is as expected because this value
     * is documented and if it changes, documentation also needs to change.
     */
    @Test
    public void shouldEnforceSizeCapSingleValueSingleType()
    {
        NamedDynamicValueGenerator[] dynamicValueGenerators = NamedDynamicValueGenerator.values();
        for ( NamedDynamicValueGenerator generator : dynamicValueGenerators )
        {
            String propKey = PROP_KEYS[0] + generator.name();
            createIndex( propKey );

            BinarySearch binarySearch = new BinarySearch();
            Object propValue;

            while ( !binarySearch.finished() )
            {
                propValue = generator.dynamicValue( binarySearch.arrayLength );
                long expectedNodeId = -1;

                // Write
                boolean wasAbleToWrite = true;
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = db.createNode( LABEL_ONE );
                    node.setProperty( propKey, propValue );
                    expectedNodeId = node.getId();
                    tx.success();
                }
                catch ( Exception e )
                {
                    wasAbleToWrite = false;
                }

                // Read
                verifyReadExpected( propKey, propValue, expectedNodeId, wasAbleToWrite );

                // Progress binary search
                binarySearch.progress( wasAbleToWrite );
            }
            assertEquals( format( "expected longest successful array length for type %s, to be %d but was %d. " +
                            "This is a strong indication that documentation of max limit needs to be updated.",
                    generator.name(), generator.expectedMax, binarySearch.longestSuccessful ), generator.expectedMax, binarySearch.longestSuccessful );
        }
    }

    private class BinarySearch
    {
        private int longestSuccessful;
        private int minArrayLength;
        private int maxArrayLength = 1;
        private int arrayLength = 1;
        private boolean foundMaxLimit;

        boolean finished()
        {
            // When arrayLength is stable on minArrayLength, our binary search for max limit is finished
            return arrayLength == minArrayLength;
        }

        void progress( boolean wasAbleToWrite )
        {
            if ( wasAbleToWrite )
            {
                longestSuccessful = Math.max( arrayLength, longestSuccessful );
                if ( !foundMaxLimit )
                {
                    // We continue to double the max limit until we find some upper limit
                    minArrayLength = arrayLength;
                    maxArrayLength *= 2;
                    arrayLength = maxArrayLength;
                }
                else
                {
                    // We where able to write so we can move min limit up to current array length
                    minArrayLength = arrayLength;
                    arrayLength = (minArrayLength + maxArrayLength) / 2;
                }
            }
            else
            {
                foundMaxLimit = true;
                // We where not able to write so we take max limit down to current array length
                maxArrayLength = arrayLength;
                arrayLength = (minArrayLength + maxArrayLength) / 2;
            }
        }
    }

    /**
     * Key size validation test for mixed types in composite index.
     *
     * Validate that we handle index reads and writes correctly for
     * dynamically sized values (arrays and strings) of all different
     * types with length close to and over the max limit for given
     * type.
     *
     * We do this by trying to insert random dynamically sized values
     * with size in range that covers the limit, taking into account
     * the number of slots in the index.
     * Then we verify that we either
     *  - write successfully and are able to read value back
     *  - fail to write and no result is found during read
     *
     * Even though we don't keep track of all inserted values, the
     * probability that we will ever generate two identical values
     * is, for single property boolean array which is the most likely,
     * (1/2)^3995. As a reference (1/2)^100 = 7.8886091e-31.
     */
    @Test
    public void shouldEnforceSizeCapMixedTypes()
    {
        for ( int numberOfSlots = 1; numberOfSlots < 5; numberOfSlots++ )
        {
            String[] propKeys = generatePropertyKeys( numberOfSlots );

            createIndex( propKeys );
            int keySizeLimitPerSlot = KEY_SIZE_LIMIT / propKeys.length - ESTIMATED_OVERHEAD_PER_SLOT;
            int wiggleRoomPerSlot = WIGGLE_ROOM / propKeys.length;
            SuccessAndFail successAndFail = new SuccessAndFail();
            for ( int i = 0; i < 1_000; i++ )
            {
                Object[] propValues = generatePropertyValues( propKeys, keySizeLimitPerSlot, wiggleRoomPerSlot );
                long expectedNodeId = -1;

                // Write
                boolean ableToWrite = true;
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = db.createNode( LABEL_ONE );
                    setProperties( propKeys, propValues, node );
                    expectedNodeId = node.getId();
                    tx.success();
                }
                catch ( Exception e )
                {
                    ableToWrite = false;
                }
                successAndFail.ableToWrite( ableToWrite );

                // Read
                verifyReadExpected( propKeys, propValues, expectedNodeId, ableToWrite );
            }
            successAndFail.verifyBothSuccessAndFail();
        }
    }

    private void setProperties( String[] propKeys, Object[] propValues, Node node )
    {
        for ( int propKey = 0; propKey < propKeys.length; propKey++ )
        {
            node.setProperty( propKeys[propKey], propValues[propKey] );
        }
    }

    private String[] generatePropertyKeys( int numberOfSlots )
    {
        String[] propKeys = new String[numberOfSlots];
        for ( int i = 0; i < numberOfSlots; i++ )
        {
            // Use different property keys for each iteration
            propKeys[i] = PROP_KEYS[i] + "numberOfSlots" + numberOfSlots;
        }
        return propKeys;
    }

    private Object[] generatePropertyValues( String[] propKeys, int keySizeLimitPerSlot, int wiggleRoomPerSlot )
    {
        Object[] propValues = new Object[propKeys.length];
        for ( int propKey = 0; propKey < propKeys.length; propKey++ )
        {
            NamedDynamicValueGenerator among = random.among( NamedDynamicValueGenerator.values() );
            propValues[propKey] = among.dynamicValue( keySizeLimitPerSlot, wiggleRoomPerSlot );
        }
        return propValues;
    }

    private void verifyReadExpected( String propKey, Object propValue, long expectedNodeId, boolean ableToWrite )
    {
        verifyReadExpected( new String[]{propKey}, new Object[]{propValue}, expectedNodeId, ableToWrite );
    }

    private void verifyReadExpected( String[] propKeys, Object[] propValues, long expectedNodeId, boolean ableToWrite )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Object> values = new HashMap<>();
            for ( int propKey = 0; propKey < propKeys.length; propKey++ )
            {
                values.put( propKeys[propKey], propValues[propKey] );
            }
            ResourceIterator<Node> nodes = db.findNodes( LABEL_ONE, values );
            if ( ableToWrite )
            {
                assertTrue( nodes.hasNext() );
                Node node = nodes.next();
                assertNotNull( node );
                assertEquals( "node id", expectedNodeId, node.getId() );
            }
            else
            {
                assertFalse( nodes.hasNext() );
            }
            tx.success();
        }
    }

    private void createIndex( String... propKeys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator indexCreator = db.schema().indexFor( LABEL_ONE );
            for ( String propKey : propKeys )
            {
                indexCreator = indexCreator.on( propKey );
            }
            indexCreator.create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private class SuccessAndFail
    {
        boolean atLeastOneSuccess;
        boolean atLeastOneFail;

        void ableToWrite( boolean ableToWrite )
        {
            if ( ableToWrite )
            {
                atLeastOneSuccess = true;
            }
            else
            {
                atLeastOneFail = true;
            }
        }

        void verifyBothSuccessAndFail()
        {
            assertTrue( "not a single successful write, need to adjust parameters", atLeastOneSuccess );
            assertTrue( "not a single failed write, need to adjust parameters", atLeastOneFail );
        }
    }

    private enum NamedDynamicValueGenerator
    {
        string( Byte.BYTES, 4036, i -> random.randomValues().nextAlphaNumericTextValue( i, i ).stringValue() ),
        byteArray( SIZE_NUMBER_BYTE, 4035, i -> random.randomValues().nextByteArrayRaw( i, i ) ),
        shortArray( SIZE_NUMBER_SHORT, 2017, i -> random.randomValues().nextShortArrayRaw( i, i ) ),
        intArray( SIZE_NUMBER_INT, 1008, i -> random.randomValues().nextIntArrayRaw( i, i ) ),
        longArray( SIZE_NUMBER_LONG, 504, i -> random.randomValues().nextLongArrayRaw( i, i ) ),
        floatArray( SIZE_NUMBER_FLOAT, 1008, i -> random.randomValues().nextFloatArrayRaw( i, i ) ),
        doubleArray( SIZE_NUMBER_DOUBLE, 504, i -> random.randomValues().nextDoubleArrayRaw( i, i ) ),
        booleanArray( SIZE_BOOLEAN, 4036, i -> random.randomValues().nextBooleanArrayRaw( i, i ) ),
        charArray( Byte.BYTES, 1345, i -> random.randomValues().nextAlphaNumericTextValue( i, i ).stringValue().toCharArray() ),
        stringArray1( SIZE_STRING_LENGTH + 1, 1345, i -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 1, 1 ) ),
        stringArray10( SIZE_STRING_LENGTH + 10, 336, i -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 10, 10 ) ),
        stringArray100( SIZE_STRING_LENGTH + 100, 39, i -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 100, 100 ) ),
        stringArray1000( SIZE_STRING_LENGTH + 1000, 4, i -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 1000, 1000 ) ),
        dateArray( SIZE_DATE, 504, i -> random.randomValues().nextDateArrayRaw( i, i ) ),
        timeArray( SIZE_ZONED_TIME, 336, i -> random.randomValues().nextTimeArrayRaw( i, i ) ),
        localTimeArray( SIZE_LOCAL_TIME, 504, i -> random.randomValues().nextLocalTimeArrayRaw( i, i ) ),
        dateTimeArray( SIZE_ZONED_DATE_TIME, 252, i -> random.randomValues().nextDateTimeArrayRaw( i, i ) ),
        localDateTimeArray( SIZE_LOCAL_DATE_TIME, 336, i -> random.randomValues().nextLocalDateTimeArrayRaw( i, i ) ),
        durationArray( SIZE_DURATION, 144, i -> random.randomValues().nextDurationArrayRaw( i, i ) ),
        periodArray( SIZE_DURATION, 144, i -> random.randomValues().nextPeriodArrayRaw( i, i ) ),
        cartesianPointArray( SIZE_GEOMETRY, 168, i -> random.randomValues().nextCartesianPointArray( i, i ).asObjectCopy() ),
        cartesian3DPointArray( SIZE_GEOMETRY, 126, i -> random.randomValues().nextCartesian3DPointArray( i, i ).asObjectCopy() ),
        geographicPointArray( SIZE_GEOMETRY, 168, i -> random.randomValues().nextGeographicPointArray( i, i ).asObjectCopy() ),
        geographic3DPointArray( SIZE_GEOMETRY, 126, i -> random.randomValues().nextGeographic3DPointArray( i, i ).asObjectCopy() );

        private final int singleArrayEntrySize;
        private final DynamicValueGenerator generator;
        private final int expectedMax;

        NamedDynamicValueGenerator( int singleArrayEntrySize, int expectedLongestArrayLength, DynamicValueGenerator generator )
        {
            this.singleArrayEntrySize = singleArrayEntrySize;
            this.expectedMax = expectedLongestArrayLength;
            this.generator = generator;
        }

        Object dynamicValue( int length )
        {
            return generator.dynamicValue( length );
        }

        Object dynamicValue( int keySizeLimit, int wiggleRoom )
        {
            int lowLimit = lowLimit( keySizeLimit, wiggleRoom, singleArrayEntrySize );
            int highLimit = highLimit( keySizeLimit, wiggleRoom, singleArrayEntrySize );
            return dynamicValue( random.intBetween( lowLimit, highLimit ) );
        }

        private int lowLimit( int keySizeLimit, int wiggleRoom, int singleEntrySize )
        {
            return (keySizeLimit - wiggleRoom) / singleEntrySize;
        }

        private int highLimit( int keySizeLimit, int wiggleRoom, int singleEntrySize )
        {
            return (keySizeLimit + wiggleRoom) / singleEntrySize;
        }

        @FunctionalInterface
        private interface DynamicValueGenerator
        {
            Object dynamicValue( int arrayLength );
        }
    }
}
