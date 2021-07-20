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

import org.eclipse.collections.api.factory.Sets;
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
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
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
import org.neo4j.test.RandomSupport;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_STRING_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith( RandomExtension.class )
public class RecordPropertyCursorTest
{
    @Inject
    protected RandomSupport random;
    @Inject
    protected FileSystemAbstraction fs;
    @Inject
    protected PageCache pageCache;
    @Inject
    protected RecordDatabaseLayout databaseLayout;

    protected NeoStores neoStores;
    protected PropertyCreator creator;
    protected NodeRecord owner;
    protected DefaultIdGeneratorFactory idGeneratorFactory;
    private CachedStoreCursors storeCursors;

    @BeforeEach
    void setup()
    {
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate(), databaseLayout.getDatabaseName() );
        neoStores = new StoreFactory( databaseLayout, Config.defaults(), idGeneratorFactory, pageCache, fs, getRecordFormats(), NullLogProvider.getInstance(),
                PageCacheTracer.NULL, writable(), Sets.immutable.empty() ).openAllNeoStores( true );
        creator = new PropertyCreator( neoStores.getPropertyStore(), new PropertyTraverser(), NULL, INSTANCE );
        owner = neoStores.getNodeStore().newRecord();
        storeCursors = new CachedStoreCursors( neoStores, NULL );
    }

    protected RecordFormats getRecordFormats()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }

    @AfterEach
    void closeStore()
    {
        storeCursors.close();
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
        Value[] values = createValues( 20, 20 ); // many enough to create multiple records in the chain
        long firstProp = storeValuesAsPropertyChain( creator, owner, values );

        // and a cycle on the second record
        PropertyStore store = neoStores.getPropertyStore();
        PropertyRecord firstRecord = getRecord( store, firstProp, NORMAL );
        long secondProp = firstRecord.getNextProp();
        PropertyRecord secondRecord = getRecord( store, secondProp, NORMAL );
        secondRecord.setNextProp( firstProp );
        try ( var cursor = storeCursors.writeCursor( PROPERTY_CURSOR ) )
        {
            store.updateRecord( secondRecord, cursor, NULL, storeCursors );
        }
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
        PropertyRecord propertyRecord = getRecord( store, firstProp, NORMAL );
        store.ensureHeavy( propertyRecord, new CachedStoreCursors( neoStores, NULL ) );
        PropertyBlock block = propertyRecord.iterator().next();
        int cycleEndRecordIndex = random.nextInt( 1, block.getValueRecords().size() - 1 );
        DynamicRecord cycle = block.getValueRecords().get( cycleEndRecordIndex );
        int cycleStartIndex = random.nextInt( cycleEndRecordIndex );
        cycle.setNextBlock( block.getValueRecords().get( cycleStartIndex ).getId() );
        try ( var cursor = storeCursors.writeCursor( DYNAMIC_STRING_STORE_CURSOR ) )
        {
            store.getStringStore().updateRecord( cycle, cursor, NULL, storeCursors );
        }
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
        assertThat( e.getMessage(), containsString( "Unable to read property value in record" ) );
        assertThat( e.getMessage(), containsString( "owner NODE:" + owner.getId() ) );
    }

    protected RecordPropertyCursor createCursor()
    {
        return new RecordPropertyCursor( neoStores.getPropertyStore(), NULL, INSTANCE );
    }

    protected void assertPropertyChain( Value[] values, long firstPropertyId, RecordPropertyCursor cursor )
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

    protected Value[] createValues()
    {
        return createValues( 1, 20 );
    }

    protected Value[] createValues( int minNumProps, int maxNumProps )
    {
        int numberOfProperties = random.nextInt( minNumProps, maxNumProps );
        Value[] values = new Value[numberOfProperties];
        for ( int key = 0; key < numberOfProperties; key++ )
        {
            values[key] = random.nextValue();
        }
        return values;
    }

    protected long storeValuesAsPropertyChain( PropertyCreator creator, NodeRecord owner, Value[] values )
    {
        DirectRecordAccessSet access = new DirectRecordAccessSet( neoStores, idGeneratorFactory, NULL );
        long firstPropertyId = creator.createPropertyChain( owner, blocksOf( creator, values ), access.getPropertyRecords() );
        access.commit();
        return firstPropertyId;
    }

    protected static Map<Integer, Value> asMap( Value[] values )
    {
        Map<Integer, Value> map = new HashMap<>();
        for ( int key = 0; key < values.length; key++ )
        {
            map.put( key, values[key] );
        }
        return map;
    }

    protected static Iterator<PropertyBlock> blocksOf( PropertyCreator creator, Value[] values )
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

    private PropertyRecord getRecord( PropertyStore propertyStore, long id, RecordLoad load )
    {
        try ( PageCursor cursor = propertyStore.openPageCursorForReading( id, NULL ) )
        {
            return propertyStore.getRecordByCursor( id, propertyStore.newRecord(), load, cursor );
        }
    }
}
