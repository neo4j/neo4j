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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.RandomValues.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
    private static final String PROP_KEY = "largeString";
    private static final int KEY_SIZE_LIMIT = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE );
    private static final int WIGGLE_ROOM = 50;
    private Types[] allValidNonArrayTypes;

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule().withSetting( default_schema_provider, NATIVE_BTREE10.providerIdentifier() );

    @Rule
    public final RandomRule random = new RandomRule();

    @Before
    public void setup()
    {
        allValidNonArrayTypes = Types.values();
        allValidNonArrayTypes = ArrayUtils.removeElement( allValidNonArrayTypes, Types.ARRAY );
        // todo include points when NATIVE_BTREE10 support spatial
        allValidNonArrayTypes = ArrayUtils.removeElement( allValidNonArrayTypes, Types.CARTESIAN_POINT );
        allValidNonArrayTypes = ArrayUtils.removeElement( allValidNonArrayTypes, Types.CARTESIAN_POINT_3D );
        allValidNonArrayTypes = ArrayUtils.removeElement( allValidNonArrayTypes, Types.GEOGRAPHIC_POINT );
        allValidNonArrayTypes = ArrayUtils.removeElement( allValidNonArrayTypes, Types.GEOGRAPHIC_POINT_3D );
    }

    @Test
    public void shouldEnforceSizeCapSingleValue()
    {
        createIndex();
        Set<Object> generatedValues = new HashSet<>();
        for ( int i = 0; i < 1_000; i++ )
        {
            Object propValue = generateSingleValueInAndAroundTheSizeCap( generatedValues );
            long expectedNodeId = -1;

            // Write
            boolean ableToWrite = true;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL_ONE );
                node.setProperty( PROP_KEY, propValue );
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
                Node node = db.findNode( LABEL_ONE, PROP_KEY, propValue );
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
    }

    // todo delete me when finished
    private void printArray( Object propValue )
    {
        if ( propValue instanceof byte[] )
        {
            System.out.println( Arrays.toString( (byte[]) propValue ) );
        }
        if ( propValue instanceof short[] )
        {
            System.out.println( Arrays.toString( (short[]) propValue ) );
        }
        if ( propValue instanceof int[] )
        {
            System.out.println( Arrays.toString( (int[]) propValue ) );
        }
        if ( propValue instanceof long[] )
        {
            System.out.println( Arrays.toString( (long[]) propValue ) );
        }
        if ( propValue instanceof float[] )
        {
            System.out.println( Arrays.toString( (float[]) propValue ) );
        }
        if ( propValue instanceof double[] )
        {
            System.out.println( Arrays.toString( (double[]) propValue ) );
        }
        if ( propValue instanceof char[] )
        {
            System.out.println( Arrays.toString( (char[]) propValue ) );
        }
        if ( propValue instanceof boolean[] )
        {
            System.out.println( Arrays.toString( (boolean[]) propValue ) );
        }
        if ( propValue instanceof Object[] )
        {
            System.out.println( Arrays.toString( (Object[]) propValue ) );
        }
    }

    private Object generateSingleValueInAndAroundTheSizeCap( Set<Object> generatedValues )
    {
        Object candidate;
        do
        {
            candidate = generateSingleValue();
        }
        while ( !generatedValues.add( candidate ) );
        return candidate;
    }

    private Object generateSingleValue()
    {
        switch ( random.among( new Types[] {STRING, ARRAY} ) )
        {
        case STRING:
            return random.nextAlphaNumericString( KEY_SIZE_LIMIT - WIGGLE_ROOM, KEY_SIZE_LIMIT + WIGGLE_ROOM );
        case ARRAY:
            return generateSingleArrayValue();
        default:
            throw new IllegalArgumentException();
        }
    }

    private Object generateSingleArrayValue()
    {
        Types type = random.among( allValidNonArrayTypes );
        switch ( type )
        {
        case BOOLEAN:
            return createRandomArray( RandomValues::nextBooleanArrayRaw, SIZE_BOOLEAN );
        case BYTE:
            return createRandomArray( RandomValues::nextByteArrayRaw, SIZE_NUMBER_BYTE );
        case SHORT:
            return createRandomArray( RandomValues::nextShortArrayRaw, SIZE_NUMBER_SHORT );
        case STRING:
            return createRandomStringArray();
        case INT:
            return createRandomArray( RandomValues::nextIntArrayRaw, SIZE_NUMBER_INT );
        case LONG:
            return createRandomArray( RandomValues::nextLongArrayRaw, SIZE_NUMBER_LONG );
        case FLOAT:
            return createRandomArray( RandomValues::nextFloatArrayRaw, SIZE_NUMBER_FLOAT );
        case DOUBLE:
            return createRandomArray( RandomValues::nextDoubleArrayRaw, SIZE_NUMBER_DOUBLE );
        case LOCAL_DATE_TIME:
            return createRandomArray( RandomValues::nextLocalDateTimeArrayRaw, SIZE_LOCAL_DATE_TIME );
        case DATE:
            return createRandomArray( RandomValues::nextDateArrayRaw, SIZE_DATE );
        case LOCAL_TIME:
            return createRandomArray( RandomValues::nextLocalTimeArrayRaw, SIZE_LOCAL_TIME );
        case PERIOD:
            return createRandomArray( RandomValues::nextPeriodArrayRaw, SIZE_DURATION );
        case DURATION:
            return createRandomArray( RandomValues::nextDurationArrayRaw, SIZE_DURATION );
        case TIME:
            return createRandomArray( RandomValues::nextTimeArrayRaw, SIZE_ZONED_TIME );
        case DATE_TIME:
            return createRandomArray( RandomValues::nextDateTimeArrayRaw, SIZE_ZONED_DATE_TIME );
        case CARTESIAN_POINT:
        case CARTESIAN_POINT_3D:
        case GEOGRAPHIC_POINT:
        case GEOGRAPHIC_POINT_3D:
            // todo create spatial arrays when NATIVE_BTREE10 support spatial
        default:
            throw new IllegalArgumentException( "Unknown type " + type );
        }
    }

    private Object createRandomStringArray()
    {
        if ( random.nextBoolean() )
        {
            // Char array with many small elements
            int singleEntrySize = SIZE_STRING_LENGTH + 1; // Alphanumeric chars are all serialized with 1 byte
            int length = random.nextInt( lowLimit( singleEntrySize ), highLimit( singleEntrySize ) );
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
            int totalLength = random.nextInt( lowLimit( 1 ), highLimit( 1 ) );
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

    private Object createRandomArray( RandomArrayFactory factory, int entrySize )
    {
        return factory.next( random.randomValues(), lowLimit( entrySize ), highLimit( entrySize ) );
    }

    @FunctionalInterface
    private interface RandomArrayFactory
    {
        Object next( RandomValues rnd, int minLength, int maxLength );
    }

    private int lowLimit( int singleEntrySize )
    {
        return (KEY_SIZE_LIMIT - WIGGLE_ROOM) / singleEntrySize;
    }

    private int highLimit( int singleEntrySize )
    {
        return (KEY_SIZE_LIMIT + WIGGLE_ROOM) / singleEntrySize;
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( PROP_KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }
}
