/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.collections.impl.factory.Iterables;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.RandomValues.Types;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_BOOLEAN;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_DATE;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_DURATION;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_LOCAL_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_LOCAL_TIME;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_NUMBER_BYTE;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_NUMBER_DOUBLE;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_NUMBER_FLOAT;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_NUMBER_INT;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_NUMBER_LONG;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_NUMBER_SHORT;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_STRING_LENGTH;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_ZONED_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.GenericKeyState.SIZE_ZONED_TIME;
import static org.neo4j.test.TestLabels.LABEL_ONE;
import static org.neo4j.values.storable.RandomValues.Types.ARRAY;
import static org.neo4j.values.storable.RandomValues.Types.STRING;

public class GenericIndexValidationIT
{
    private static final String string = "string";
    private static final String byteArray = "byteArray";
    private static final String intArray = "intArray";
    private static final String shortArray = "shortArray";
    private static final String longArray = "longArray";
    private static final String floatArray = "floatArray";
    private static final String doubleArray = "doubleArray";
    private static final String booleanArray = "booleanArray";
    private static final String stringArray0 = "stringArray0";
    private static final String stringArray10 = "stringArray10";
    private static final String stringArray100 = "stringArray100";
    private static final String stringArray1000 = "stringArray1000";
    private static final String dateArray = "dateArray";
    private static final String timeArray = "timeArray";
    private static final String localTimeArray = "localTimeArray";
    private static final String dateTimeArray = "dateTimeArray";
    private static final String localDateTimeArray = "localDateTimeArray";
    private static final String durationArray = "durationArray";
    private static final String periodArray = "periodArray";

    private static final String[] PROP_KEYS = new String[]{
            "prop0",
            "prop1",
            "prop2",
            "prop3",
            "prop4",
            "prop5",
            "prop6",
            "prop7",
            "prop8",
            "prop9"
    };
    private static final int KEY_SIZE_LIMIT = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE );
    private static final int ESTIMATED_OVERHEAD_PER_SLOT = 2;
    private static final int WIGGLE_ROOM = 50;
    private List<Types> allValidNonArrayTypes = Iterables.mList( Types.values() )
            .without( Types.ARRAY )
            .without( Types.CARTESIAN_POINT )
            .without( Types.CARTESIAN_POINT_3D )
            .without( Types.GEOGRAPHIC_POINT )
            .without( Types.GEOGRAPHIC_POINT_3D );

    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule().withSetting( default_schema_provider, NATIVE_BTREE10.providerIdentifier() );

    @ClassRule
    public static RandomRule random = new RandomRule();

    @Test
    public void shouldEnforceSizeCapSingleValue()
    {
        createIndex( PROP_KEYS[0] );
        int keySizeLimitSingleSlot = KEY_SIZE_LIMIT - ESTIMATED_OVERHEAD_PER_SLOT;
        for ( int i = 0; i < 1_000; i++ )
        {
            Object propValue = generateSingleValue( keySizeLimitSingleSlot, WIGGLE_ROOM );
            long expectedNodeId = -1;

            // Write
            boolean ableToWrite = true;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL_ONE );
                node.setProperty( PROP_KEYS[0], propValue );
                expectedNodeId = node.getId();
                tx.success();
            }
            catch ( Exception e )
            {
                ableToWrite = false;
            }

            // Read
            verifyReadExpected( PROP_KEYS[0], propValue, expectedNodeId, ableToWrite );
        }
    }

    /**
     * Validate that we handle index reads and writes correctly for arrays of all different types
     * with length close to and over the max limit for given type.
     * We do this by inserting arrays of increasing size (doubling each iteration) and when we hit the upper limit
     * we do binary search between the established min and max limit.
     * We also verify that the largest successful array length for each type is as expected because this value
     * is documented and if it changes, documentation also needs to change.
     */
    @Test
    public void shouldEnforceSizeCapSingleArray()
    {
        NamedDynamicValueGenerator[] dynamicValueGenerators = dynamicValueGenerators();
        for ( NamedDynamicValueGenerator generator : dynamicValueGenerators )
        {
            String propKey = PROP_KEYS[0] + generator.name();
            createIndex( propKey );

            int longestSuccessful = 0;
            int minArrayLength = 0;
            int maxArrayLength = 1;
            int arrayLength = 1;
            boolean foundMaxLimit = false;
            Object propValue;

            // When arrayLength is stable on minArrayLength, our binary search for max limit is finished
            while ( arrayLength != minArrayLength )
            {
                propValue = generator.dynamicValue( arrayLength );
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
                    foundMaxLimit = true;
                    wasAbleToWrite = false;
                }

                // Read
                verifyReadExpected( propKey, propValue, expectedNodeId, wasAbleToWrite );

                // We try to do binary search to find the exact array length limit for current type
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
                    // We where not able to write so we take max limit down to current array length
                    maxArrayLength = arrayLength;
                    arrayLength = (minArrayLength + maxArrayLength) / 2;
                }
            }
            int expectedLongest;
            switch ( generator.name() )
            {
            case string:
                expectedLongest = 4036;
                break;
            case byteArray:
                expectedLongest = 4033;
                break;
            case shortArray:
                expectedLongest = 2016;
                break;
            case intArray:
                expectedLongest = 1008;
                break;
            case longArray:
                expectedLongest = 504;
                break;
            case floatArray:
                expectedLongest = 1008;
                break;
            case doubleArray:
                expectedLongest = 504;
                break;
            case booleanArray:
                expectedLongest = 4034;
                break;
            case stringArray0:
                expectedLongest = 2017;
                break;
            case stringArray10:
                expectedLongest = 336;
                break;
            case stringArray100:
                expectedLongest = 39;
                break;
            case stringArray1000:
                expectedLongest = 4;
                break;
            case dateArray:
                expectedLongest = 504;
                break;
            case timeArray:
                expectedLongest = 336;
                break;
            case localTimeArray:
                expectedLongest = 504;
                break;
            case dateTimeArray:
                expectedLongest = 252;
                break;
            case localDateTimeArray:
                expectedLongest = 336;
                break;
            case durationArray:
                expectedLongest = 144;
                break;
            case periodArray:
                expectedLongest = 144;
                break;
            default:
                throw new IllegalArgumentException( "Did not recognize type, " + generator.name() +
                        ". Please add new type to this list of expected array lengths if you have added a new type." );
            }
            assertEquals( format( "expected longest successful array length for type %s, to be %d but was %d. " +
                            "This is a strong indication that documentation of max limit needs to be updated.",
                    generator.name(), expectedLongest, longestSuccessful ), expectedLongest, longestSuccessful );
        }
    }

    @Test
    public void shouldEnforceSizeCapComposite()
    {
        createIndex( PROP_KEYS );
        int keySizeLimitPerSlot = KEY_SIZE_LIMIT / PROP_KEYS.length - ESTIMATED_OVERHEAD_PER_SLOT;
        int wiggleRoomPerSlot = WIGGLE_ROOM / PROP_KEYS.length;
        for ( int i = 0; i < 1_000; i++ )
        {
            Object[] propValues = new Object[PROP_KEYS.length];
            for ( int propKey = 0; propKey < PROP_KEYS.length; propKey++ )
            {
                propValues[propKey] = generateSingleValue( keySizeLimitPerSlot, wiggleRoomPerSlot );
            }
            long expectedNodeId = -1;

            // Write
            boolean ableToWrite = true;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL_ONE );
                for ( int propKey = 0; propKey < PROP_KEYS.length; propKey++ )
                {
                    node.setProperty( PROP_KEYS[propKey], propValues[propKey] );
                }
                expectedNodeId = node.getId();
                tx.success();
            }
            catch ( Exception e )
            {
                ableToWrite = false;
            }

            // Read
            try ( Transaction tx = db.beginTx() )
            {
                Map<String,Object> values = new HashMap<>();
                for ( int propKey = 0; propKey < PROP_KEYS.length; propKey++ )
                {
                    values.put( PROP_KEYS[propKey], propValues[propKey] );
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
    }

    private void verifyReadExpected( String propKey, Object propValue, long expectedNodeId, boolean ableToWrite )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.findNode( LABEL_ONE, propKey, propValue );
            if ( ableToWrite )
            {
                assertNotNull( node );
                assertEquals( "node id", expectedNodeId, node.getId() );
            }
            else
            {
                assertNull( node );
            }
            tx.success();
        }
    }

    private static NamedDynamicValueGenerator[] dynamicValueGenerators()
    {
        return new NamedDynamicValueGenerator[]{
                new NamedDynamicValueGenerator( string, ( i ) -> random.randomValues().nextAlphaNumericTextValue( i, i ).stringValue() ),
                new NamedDynamicValueGenerator( byteArray, ( i ) -> random.randomValues().nextByteArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( shortArray, ( i ) -> random.randomValues().nextShortArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( intArray, ( i ) -> random.randomValues().nextIntArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( longArray, ( i ) -> random.randomValues().nextLongArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( floatArray, ( i ) -> random.randomValues().nextFloatArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( doubleArray, ( i ) -> random.randomValues().nextDoubleArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( booleanArray, ( i ) -> random.randomValues().nextBooleanArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( stringArray0, ( i ) -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 0, 0 ) ),
                new NamedDynamicValueGenerator( stringArray10, ( i ) -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 10, 10 ) ),
                new NamedDynamicValueGenerator( stringArray100, ( i ) -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 100, 100 ) ),
                new NamedDynamicValueGenerator( stringArray1000, ( i ) -> random.randomValues().nextAlphaNumericStringArrayRaw( i, i, 1000, 1000 ) ),
                new NamedDynamicValueGenerator( dateArray, ( i ) -> random.randomValues().nextDateArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( timeArray, ( i ) -> random.randomValues().nextTimeArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( localTimeArray, ( i ) -> random.randomValues().nextLocalTimeArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( dateTimeArray, ( i ) -> random.randomValues().nextDateTimeArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( localDateTimeArray, ( i ) -> random.randomValues().nextLocalDateTimeArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( durationArray, ( i ) -> random.randomValues().nextDurationArrayRaw( i, i ) ),
                new NamedDynamicValueGenerator( periodArray, ( i ) -> random.randomValues().nextPeriodArrayRaw( i, i ) )
                // TODO Point (Cartesian)
                // TODO Point (Cartesian 3D)
                // TODO Point (WGS-84)
                // TODO Point (WGS-84 3D)
        };
    }

    private static class NamedDynamicValueGenerator implements DynamicValueGenerator
    {
        private final String name;
        private final DynamicValueGenerator generator;

        NamedDynamicValueGenerator( String name, DynamicValueGenerator generator )
        {
            this.name = name;
            this.generator = generator;
        }
        String name()
        {
            return name;
        }

        @Override
        public Object dynamicValue( int arrayLength )
        {
            return generator.dynamicValue( arrayLength );
        }
    }

    private Object generateSingleValue( int keySizeLimit, int wiggleRoom )
    {
        switch ( random.among( new Types[] {STRING, ARRAY} ) )
        {
        case STRING:
            return random.nextAlphaNumericString( keySizeLimit - wiggleRoom, keySizeLimit + wiggleRoom );
        case ARRAY:
            return generateSingleArrayValue( keySizeLimit, wiggleRoom );
        default:
            throw new IllegalArgumentException();
        }
    }

    private Object generateSingleArrayValue( int keySizeLimit, int wiggleRoom )
    {
        Types type = random.among( allValidNonArrayTypes );
        switch ( type )
        {
        case BOOLEAN:
            return createRandomArray( RandomValues::nextBooleanArrayRaw, keySizeLimit, wiggleRoom, SIZE_BOOLEAN );
        case BYTE:
            return createRandomArray( RandomValues::nextByteArrayRaw, keySizeLimit, wiggleRoom, SIZE_NUMBER_BYTE );
        case SHORT:
            return createRandomArray( RandomValues::nextShortArrayRaw, keySizeLimit, wiggleRoom, SIZE_NUMBER_SHORT );
        case STRING:
            return createRandomStringArray( keySizeLimit, wiggleRoom );
        case INT:
            return createRandomArray( RandomValues::nextIntArrayRaw, keySizeLimit, wiggleRoom, SIZE_NUMBER_INT );
        case LONG:
            return createRandomArray( RandomValues::nextLongArrayRaw, keySizeLimit, wiggleRoom, SIZE_NUMBER_LONG );
        case FLOAT:
            return createRandomArray( RandomValues::nextFloatArrayRaw, keySizeLimit, wiggleRoom, SIZE_NUMBER_FLOAT );
        case DOUBLE:
            return createRandomArray( RandomValues::nextDoubleArrayRaw, keySizeLimit, wiggleRoom, SIZE_NUMBER_DOUBLE );
        case LOCAL_DATE_TIME:
            return createRandomArray( RandomValues::nextLocalDateTimeArrayRaw, keySizeLimit, wiggleRoom, SIZE_LOCAL_DATE_TIME );
        case DATE:
            return createRandomArray( RandomValues::nextDateArrayRaw, keySizeLimit, wiggleRoom, SIZE_DATE );
        case LOCAL_TIME:
            return createRandomArray( RandomValues::nextLocalTimeArrayRaw, keySizeLimit, wiggleRoom, SIZE_LOCAL_TIME );
        case PERIOD:
            return createRandomArray( RandomValues::nextPeriodArrayRaw, keySizeLimit, wiggleRoom, SIZE_DURATION );
        case DURATION:
            return createRandomArray( RandomValues::nextDurationArrayRaw, keySizeLimit, wiggleRoom, SIZE_DURATION );
        case TIME:
            return createRandomArray( RandomValues::nextTimeArrayRaw, keySizeLimit, wiggleRoom, SIZE_ZONED_TIME );
        case DATE_TIME:
            return createRandomArray( RandomValues::nextDateTimeArrayRaw, keySizeLimit, wiggleRoom, SIZE_ZONED_DATE_TIME );
        case CARTESIAN_POINT:
        case CARTESIAN_POINT_3D:
        case GEOGRAPHIC_POINT:
        case GEOGRAPHIC_POINT_3D:
            // todo create spatial arrays when NATIVE_BTREE10 support spatial
        default:
            throw new IllegalArgumentException( "Unknown type " + type );
        }
    }

    private Object createRandomStringArray( int keySizeLimit, int wiggleRoom )
    {
        if ( random.nextBoolean() )
        {
            // Char array with many small elements
            int singleEntrySize = SIZE_STRING_LENGTH + 1; // Alphanumeric chars are all serialized with 1 byte
            int length = random.nextInt( lowLimit( keySizeLimit, wiggleRoom, singleEntrySize ), highLimit( keySizeLimit, wiggleRoom, singleEntrySize ) );
            char[] chars = new char[length];
            for ( int i = 0; i < chars.length; i++ )
            {
                chars[i] = RandomStringUtils.randomAlphanumeric( 1 ).charAt( 0 );
            }
            return chars;
        }
        else
        {
            // String array with few large elements
            // We generate only a small number of large strings so overhead per char is negligible.
            int totalLength = random.nextInt( lowLimit( keySizeLimit, wiggleRoom, 1 ), highLimit( keySizeLimit, wiggleRoom, 1 ) );
            int maxNumberOfStrings = 4;
            List<String> strings = new ArrayList<>();
            while ( strings.size() < maxNumberOfStrings - 1 && totalLength > 0 )
            {
                int sizeOfString = random.nextInt( totalLength );
                strings.add( RandomStringUtils.randomAlphanumeric( sizeOfString ) );
                totalLength -= sizeOfString;
            }
            strings.add( RandomStringUtils.randomAlphabetic( totalLength ) );
            return strings.toArray( new String[strings.size()] );
        }
    }

    private Object createRandomArray( RandomArrayFactory factory, int keySizeLimit, int wiggleRoom, int entrySize )
    {
        return factory.next( random.randomValues(), lowLimit( keySizeLimit, wiggleRoom, entrySize ), highLimit( keySizeLimit, wiggleRoom, entrySize ) );
    }

    private int lowLimit( int keySizeLimit, int wiggleRoom, int singleEntrySize )
    {
        return (keySizeLimit - wiggleRoom) / singleEntrySize;
    }

    private int highLimit( int keySizeLimit, int wiggleRoom, int singleEntrySize )
    {
        return (keySizeLimit + wiggleRoom) / singleEntrySize;
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

    @FunctionalInterface
    private interface DynamicValueGenerator
    {
        Object dynamicValue( int arrayLength );
    }

    @FunctionalInterface
    private interface RandomArrayFactory
    {
        Object next( RandomValues rnd, int minLength, int maxLength );
    }
}
