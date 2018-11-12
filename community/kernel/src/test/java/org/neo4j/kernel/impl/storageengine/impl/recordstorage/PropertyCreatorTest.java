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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DirectRecordAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@PageCacheExtension
class PropertyCreatorTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    private final MyPrimitiveProxy primitive = new MyPrimitiveProxy();
    private NeoStores neoStores;
    private PropertyStore propertyStore;
    private PropertyCreator creator;
    private DirectRecordAccess<PropertyRecord,PrimitiveRecord> records;

    @BeforeEach
    void startStore()
    {
        neoStores = new StoreFactory( testDirectory.databaseLayout(), Config.defaults(), new DefaultIdGeneratorFactory( fileSystem ),
                pageCache, fileSystem, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY ).openNeoStores( true,
                StoreType.PROPERTY, StoreType.PROPERTY_STRING, StoreType.PROPERTY_ARRAY );
        propertyStore = neoStores.getPropertyStore();
        records = new DirectRecordAccess<>( propertyStore, Loaders.propertyLoader( propertyStore ) );
        creator = new PropertyCreator( propertyStore, new PropertyTraverser() );
    }

    @AfterEach
    void closeStore()
    {
        neoStores.close();
    }

    @Test
    void shouldAddPropertyToEmptyChain()
    {
        // GIVEN
        existingChain();

        // WHEN
        setProperty( 1, "value" );

        // THEN
        assertChain( record( property( 1, "value" ) ) );
    }

    @Test
    void shouldAddPropertyToChainContainingOtherFullRecords()
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ), property( 7, 7 ) ) );

        // WHEN
        setProperty( 10, 10 );

        // THEN
        assertChain(
                record( property( 10, 10 ) ),
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ), property( 7, 7 ) ) );
    }

    @Test
    void shouldAddPropertyToChainContainingOtherNonFullRecords()
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ) ) );

        // WHEN
        setProperty( 10, 10 );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 6, 6 ), property( 10, 10 ) ) );
    }

    @Test
    void shouldAddPropertyToChainContainingOtherNonFullRecordsInMiddle()
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ) ),
                record( property( 3, 3 ), property( 4, 4 ), property( 5, 5 ), property( 6, 6 ) ) );

        // WHEN
        setProperty( 10, 10 );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 10, 10 ) ),
                record( property( 3, 3 ), property( 4, 4 ), property( 5, 5 ), property( 6, 6 ) ) );
    }

    @Test
    void shouldChangeOnlyProperty()
    {
        // GIVEN
        existingChain( record( property( 0, "one" ) ) );

        // WHEN
        setProperty( 0, "two" );

        // THEN
        assertChain( record( property( 0, "two" ) ) );
    }

    @Test
    void shouldChangePropertyInChainWithOthersBeforeIt()
    {
        // GIVEN
        existingChain(
                record( property( 0, "one" ), property( 1, 1 ) ),
                record( property( 2, "two" ), property( 3, 3 ) ) );

        // WHEN
        setProperty( 2, "two*" );

        // THEN
        assertChain(
                record( property( 0, "one" ), property( 1, 1 ) ),
                record( property( 2, "two*" ), property( 3, 3 ) ) );
    }

    @Test
    void shouldChangePropertyInChainWithOthersAfterIt()
    {
        // GIVEN
        existingChain(
                record( property( 0, "one" ), property( 1, 1 ) ),
                record( property( 2, "two" ), property( 3, 3 ) ) );

        // WHEN
        setProperty( 0, "one*" );

        // THEN
        assertChain(
                record( property( 0, "one*" ), property( 1, 1 ) ),
                record( property( 2, "two" ), property( 3, 3 ) ) );
    }

    @Test
    void shouldChangePropertyToBiggerInFullChain()
    {
        // GIVEN
        existingChain( record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ) );

        // WHEN
        setProperty( 1, Long.MAX_VALUE );

        // THEN
        assertChain(
                record( property( 1, Long.MAX_VALUE ) ),
                record( property( 0, 0 ), property( 2, 2 ), property( 3, 3 ) ) );
    }

    @Test
    void shouldChangePropertyToBiggerInChainWithHoleAfter()
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ) ) );

        // WHEN
        setProperty( 1, Long.MAX_VALUE );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 2, 2 ), property( 3, 3 ) ),
                record( property( 4, 4 ), property( 5, 5 ), property( 1, Long.MAX_VALUE ) ) );
    }

    // change property so that it gets bigger and fits in a record earlier in the chain
    @Test
    void shouldChangePropertyToBiggerInChainWithHoleBefore()
    {
        // GIVEN
        existingChain(
                record( property( 0, 0 ), property( 1, 1 ) ),
                record( property( 2, 2 ), property( 3, 3 ), property( 4, 4 ), property( 5, 5 ) ) );

        // WHEN
        setProperty( 2, Long.MAX_VALUE );

        // THEN
        assertChain(
                record( property( 0, 0 ), property( 1, 1 ), property( 2, Long.MAX_VALUE ) ),
                record( property( 3, 3 ), property( 4, 4 ), property( 5, 5 ) ) );
    }

    @Test
    void canAddMultipleShortStringsToTheSameNode()
    {
        // GIVEN
        existingChain();

        // WHEN
        setProperty( 0, "value" );
        setProperty( 1, "esrever" );

        // THEN
        assertChain( record( property( 0, "value", false ), property( 1, "esrever", false ) ) );
    }

    @Test
    void canUpdateShortStringInplace()
    {
        // GIVEN
        existingChain( record( property( 0, "value" ) ) );

        // WHEN
        long before = propertyRecordsInUse();
        setProperty( 0, "other" );
        long after = propertyRecordsInUse();

        // THEN
        assertChain( record( property( 0, "other" ) ) );
        assertEquals( before, after );
    }

    @Test
    void canReplaceLongStringWithShortString()
    {
        // GIVEN
        long recordCount = dynamicStringRecordsInUse();
        long propCount = propertyRecordsInUse();
        existingChain( record( property( 0, "this is a really long string, believe me!" ) ) );
        assertEquals( recordCount + 1, dynamicStringRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );

        // WHEN
        setProperty( 0, "value" );

        // THEN
        assertChain( record( property( 0, "value", false ) ) );
        assertEquals( recordCount + 1, dynamicStringRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
    }

    @Test
    void canReplaceShortStringWithLongString()
    {
        // GIVEN
        long recordCount = dynamicStringRecordsInUse();
        long propCount = propertyRecordsInUse();
        existingChain( record( property( 0, "value" ) ) );
        assertEquals( recordCount, dynamicStringRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );

        // WHEN
        String longString = "this is a really long string, believe me!";
        setProperty( 0, longString );

        // THEN
        assertChain( record( property( 0, longString, true ) ) );
        assertEquals( recordCount + 1, dynamicStringRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
    }

    private void existingChain( ExpectedRecord... initialRecords )
    {
        PropertyRecord prev = null;
        for ( ExpectedRecord initialRecord : initialRecords )
        {
            PropertyRecord record = this.records.create( propertyStore.nextId(), primitive.record ).forChangingData();
            record.setInUse( true );
            existingRecord( record, initialRecord );

            if ( prev == null )
            {
                // This is the first one, update primitive to point to this
                primitive.record.setNextProp( record.getId() );
            }
            else
            {
                // link property records together
                record.setPrevProp( prev.getId() );
                prev.setNextProp( record.getId() );
            }

            prev = record;
        }
    }

    private void existingRecord( PropertyRecord record, ExpectedRecord initialRecord )
    {
        for ( ExpectedProperty initialProperty : initialRecord.properties )
        {
            PropertyBlock block = new PropertyBlock();
            propertyStore.encodeValue( block, initialProperty.key, initialProperty.value );
            record.addPropertyBlock( block );
        }
        assertTrue( record.size() <= PropertyType.getPayloadSize() );
    }

    private void setProperty( int key, Object value )
    {
        creator.primitiveSetProperty( primitive, key, Values.of( value ), records );
    }

    private void assertChain( ExpectedRecord... expectedRecords )
    {
        long nextProp = primitive.forReadingLinkage().getNextProp();
        int expectedRecordCursor = 0;
        while ( !Record.NO_NEXT_PROPERTY.is( nextProp ) )
        {
            PropertyRecord record = records.getIfLoaded( nextProp ).forReadingData();
            assertRecord( record, expectedRecords[expectedRecordCursor++] );
            nextProp = record.getNextProp();
        }
    }

    private void assertRecord( PropertyRecord record, ExpectedRecord expectedRecord )
    {
        assertEquals( expectedRecord.properties.length, record.numberOfProperties() );
        for ( ExpectedProperty expectedProperty : expectedRecord.properties )
        {
            PropertyBlock block = record.getPropertyBlock( expectedProperty.key );
            assertNotNull( block );
            assertEquals( expectedProperty.value, block.getType().value( block, propertyStore ) );
            if ( expectedProperty.assertHasDynamicRecords != null )
            {
                if ( expectedProperty.assertHasDynamicRecords )
                {
                    assertThat( block.getValueRecords().size(), Matchers.greaterThan( 0 ) );
                }
                else
                {
                    assertEquals( 0, block.getValueRecords().size() );
                }
            }
        }
    }

    private static class ExpectedProperty
    {
        private final int key;
        private final Value value;
        private final Boolean assertHasDynamicRecords;

        ExpectedProperty( int key, Object value )
        {
            this( key, value, null /*don't care*/ );
        }

        ExpectedProperty( int key, Object value, Boolean assertHasDynamicRecords )
        {
            this.key = key;
            this.value = Values.of( value );
            this.assertHasDynamicRecords = assertHasDynamicRecords;
        }
    }

    private static class ExpectedRecord
    {
        private final ExpectedProperty[] properties;

        ExpectedRecord( ExpectedProperty... properties )
        {
            this.properties = properties;
        }
    }

    private static ExpectedProperty property( int key, Object value )
    {
        return new ExpectedProperty( key, value );
    }

    private static ExpectedProperty property( int key, Object value, boolean hasDynamicRecords )
    {
        return new ExpectedProperty( key, value, hasDynamicRecords );
    }

    private static ExpectedRecord record( ExpectedProperty... properties )
    {
        return new ExpectedRecord( properties );
    }

    private static class MyPrimitiveProxy implements RecordProxy<NodeRecord,Void>
    {
        private final NodeRecord record = new NodeRecord( 5 );
        private boolean changed;

        MyPrimitiveProxy()
        {
            record.setInUse( true );
        }

        @Override
        public long getKey()
        {
            return record.getId();
        }

        @Override
        public NodeRecord forChangingLinkage()
        {
            changed = true;
            return record;
        }

        @Override
        public NodeRecord forChangingData()
        {
            changed = true;
            return record;
        }

        @Override
        public NodeRecord forReadingLinkage()
        {
            return record;
        }

        @Override
        public NodeRecord forReadingData()
        {
            return record;
        }

        @Override
        public Void getAdditionalData()
        {
            return null;
        }

        @Override
        public NodeRecord getBefore()
        {
            return record;
        }

        @Override
        public boolean isChanged()
        {
            return changed;
        }

        @Override
        public boolean isCreated()
        {
            return false;
        }
    }

    private long propertyRecordsInUse()
    {
        return propertyStore.getHighId();
    }

    private long dynamicStringRecordsInUse()
    {
        return propertyStore.getStringStore().getHighId();
    }
}
