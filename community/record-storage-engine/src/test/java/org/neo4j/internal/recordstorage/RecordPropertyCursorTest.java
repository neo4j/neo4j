/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.IteratorWrapper;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class RecordPropertyCursorTest
{
    @Inject
    private RandomRule random;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private PropertyCreator creator;
    private NodeRecord owner;
    private DefaultIdGeneratorFactory idGeneratorFactory;

    @BeforeEach
    void setup()
    {
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        neoStores = new StoreFactory( databaseLayout, Config.defaults(), idGeneratorFactory,
                pageCache, fs, NullLogProvider.getInstance() ).openAllNeoStores( true );
        creator = new PropertyCreator( neoStores.getPropertyStore(), new PropertyTraverser() );
        owner = neoStores.getNodeStore().newRecord();
    }

    @AfterEach
    void closeStore()
    {
        neoStores.close();
    }

    @Test
    void shouldReadPropertyChain()
    {
        // given
        Value[] values = createValues();
        long firstPropertyId = storeValuesAsPropertyChain( creator, owner, values );

        // when
        assertPropertyChain( values, firstPropertyId, createCursor() );
    }

    @Test
    void shouldReuseCursor()
    {
        // given
        Value[] valuesA = createValues();
        long firstPropertyIdA = storeValuesAsPropertyChain( creator, owner, valuesA );
        Value[] valuesB = createValues();
        long firstPropertyIdB = storeValuesAsPropertyChain( creator, owner, valuesB );

        // then
        RecordPropertyCursor cursor = createCursor();
        assertPropertyChain( valuesA, firstPropertyIdA, cursor );
        assertPropertyChain( valuesB, firstPropertyIdB, cursor );
    }

    @Test
    void closeShouldBeIdempotent()
    {
        // given
        RecordPropertyCursor cursor = createCursor();

        // when
        cursor.close();

        // then
        cursor.close();
    }

    @Test
    void shouldAbortChainTraversalOnLikelyCycle()
    {
        // given
        Value[] values = createValues( 20 ); // many enough to create multiple records in the chain
        long firstProp = storeValuesAsPropertyChain( creator, owner, values );

        // and a cycle on the second record
        PropertyStore store = neoStores.getPropertyStore();
        PropertyRecord firstRecord = store.getRecord( firstProp, store.newRecord(), RecordLoad.NORMAL );
        long secondProp = firstRecord.getNextProp();
        PropertyRecord secondRecord = store.getRecord( secondProp, store.newRecord(), RecordLoad.NORMAL );
        secondRecord.setNextProp( firstProp );
        store.updateRecord( secondRecord );
        owner.setId( 99 );

        // when
        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties( firstProp, owner.getId() );
        InconsistentDataReadException e = assertThrows( InconsistentDataReadException.class, () ->
        {
            while ( cursor.next() )
            {
                // just keep going, it should eventually hit the cycle detection threshold
            }
        } );

        // then
        assertEquals( format( "Aborting property reading due to detected chain cycle, starting at property record id:%d from owner NODE:%d", firstProp,
                owner.getId() ), e.getMessage() );
    }

    @Test
    void shouldAbortChainTraversalOnLikelyDynamicValueCycle()
    {
        // given
        Value value = random.nextAlphaNumericTextValue( 1000, 1000 );
        long firstProp = storeValuesAsPropertyChain( creator, owner, new Value[]{value} );

        // and a cycle on the second record
        PropertyStore store = neoStores.getPropertyStore();
        PropertyRecord propertyRecord = store.getRecord( firstProp, store.newRecord(), RecordLoad.NORMAL );
        store.ensureHeavy( propertyRecord );
        PropertyBlock block = propertyRecord.iterator().next();
        int cycleEndRecordIndex = random.nextInt( 1, block.getValueRecords().size() - 1 );
        DynamicRecord cycle = block.getValueRecords().get( cycleEndRecordIndex );
        int cycleStartIndex = random.nextInt( cycleEndRecordIndex );
        cycle.setNextBlock( block.getValueRecords().get( cycleStartIndex ).getId() );
        store.getStringStore().updateRecord( cycle );
        owner.setId( 99 );

        // when
        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties( firstProp, owner.getId() );
        InconsistentDataReadException e = assertThrows( InconsistentDataReadException.class, () ->
        {
            while ( cursor.next() )
            {
                // just keep going, it should eventually hit the cycle detection threshold
                cursor.propertyValue();
            }
        } );

        // then
        assertThat( e.getMessage(), containsString( "Unable to read property value in" ) );
        assertThat( e.getMessage(), containsString( "owner NODE:" + owner.getId() ) );
    }

    private RecordPropertyCursor createCursor()
    {
        return new RecordPropertyCursor( neoStores.getPropertyStore() );
    }

    private void assertPropertyChain( Value[] values, long firstPropertyId, RecordPropertyCursor cursor )
    {
        Map<Integer, Value> expectedValues = asMap( values );
        // This is a specific test for RecordPropertyCursor and we know that node/relationships init methods are the same
        cursor.initNodeProperties( firstPropertyId, owner.getId() );
        while ( cursor.next() )
        {
            // then
            assertEquals( expectedValues.remove( cursor.propertyKey() ), cursor.propertyValue() );
        }
        assertTrue( expectedValues.isEmpty() );
    }

    private Value[] createValues()
    {
        return createValues( random.nextInt( 1, 20 ) );
    }

    private Value[] createValues( int numberOfProperties )
    {
        Value[] values = new Value[numberOfProperties];
        for ( int key = 0; key < numberOfProperties; key++ )
        {
            values[key] = random.nextValue();
        }
        return values;
    }

    private long storeValuesAsPropertyChain( PropertyCreator creator, NodeRecord owner, Value[] values )
    {
        DirectRecordAccessSet access = new DirectRecordAccessSet( neoStores, idGeneratorFactory );
        long firstPropertyId = creator.createPropertyChain( owner, blocksOf( creator, values ), access.getPropertyRecords() );
        access.close();
        return firstPropertyId;
    }

    private static Map<Integer, Value> asMap( Value[] values )
    {
        Map<Integer, Value> map = new HashMap<>();
        for ( int key = 0; key < values.length; key++ )
        {
            map.put( key, values[key] );
        }
        return map;
    }

    private static Iterator<PropertyBlock> blocksOf( PropertyCreator creator, Value[] values )
    {
        return new IteratorWrapper<>( iterator( values ) )
        {
            int key;

            @Override
            protected PropertyBlock underlyingObjectToObject( Value value )
            {
                return creator.encodePropertyValue( key++, value );
            }
        };
    }
}
