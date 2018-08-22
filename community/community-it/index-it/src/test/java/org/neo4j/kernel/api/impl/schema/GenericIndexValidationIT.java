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
    public final DatabaseRule db = new EmbeddedDatabaseRule().withSetting( default_schema_provider, NATIVE_BTREE10.providerIdentifier() );

    @Rule
    public final RandomRule random = new RandomRule();

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
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.findNode( LABEL_ONE, PROP_KEYS[0], propValue );
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

    @FunctionalInterface
    private interface RandomArrayFactory
    {
        Object next( RandomValues rnd, int minLength, int maxLength );
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
}
