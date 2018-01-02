/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.IteratorUtil.asList;


public abstract class RecordStoreConsistentReadTest<R extends AbstractBaseRecord, S extends RecordStore<R>>
{
    // Constants for the contents of the existing record
    protected static final int ID = 1;

    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule( false );

    private FileSystemAbstraction fs;
    private AtomicBoolean nextReadIsInconsistent;

    @Before
    public void setUp()
    {
        fs = new EphemeralFileSystemAbstraction();
        nextReadIsInconsistent = new AtomicBoolean();
    }

    private NeoStores storeFixture()
    {
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        pageCache = pageCacheRule.withInconsistentReads( pageCache, nextReadIsInconsistent );
        File storeDir = new File( "stores" );
        StoreFactory factory = new StoreFactory( fs, storeDir, pageCache, NullLogProvider.getInstance() );
        NeoStores neoStores = factory.openAllNeoStores( true );
        S store = initialiseStore( neoStores );

        CommonAbstractStore commonAbstractStore = (CommonAbstractStore) store;
        commonAbstractStore.rebuildIdGenerator();
        return neoStores;
    }

    protected S initialiseStore( NeoStores neoStores )
    {
        S store = getStore( neoStores );
        store.updateRecord( createExistingRecord( false, false ) );
        return store;
    }

    protected abstract S getStore( NeoStores neoStores );

    protected abstract R createNullRecord( long id );

    protected abstract R createExistingRecord( boolean forced, boolean light );

    protected abstract R getLight( long id, S store );

    protected abstract void assertRecordsEqual( R actualRecord, R expectedRecord );

    protected R getHeavy( S store, int id )
    {
        return store.getRecord( id );
    }

    protected R getForce( S store, int id )
    {
        return store.forceGetRecord( id );
    }

    @Test
    public void mustReadExistingRecord()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            R record = getHeavy( store, ID );
            assertRecordsEqual( record, createExistingRecord( false, false ) );
        }
    }

    @Test
    public void mustReadExistingLightRecord()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            R record = getLight( ID, store );
            assertRecordsEqual( record, createExistingRecord( false, true ) );
        }
    }

    @Test
    public void mustForceReadExistingRecord()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            R record = getForce( store, ID );
            assertRecordsEqual( record, createExistingRecord( true, false ) );
        }
    }

    @Test( expected = InvalidRecordException.class )
    public void readingNonExistingRecordMustThrow()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            getHeavy( store, ID + 1 );
        }
    }

    @Test
    public void readingNonExistingLightRecordMustReturnNull()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            R record = getLight( ID + 1, store );
            assertNull( record );
        }
    }

    @Test
    public void forceReadingNonExistingRecordMustReturnEmptyRecordWithThatId()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            R record = getForce( store, ID + 1 );
            R nullRecord = createNullRecord( ID + 1 );
            assertRecordsEqual( record, nullRecord );
        }
    }

    @Test
    public void mustRetryInconsistentReads()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            nextReadIsInconsistent.set( true );
            R record = getHeavy( store, ID );
            assertRecordsEqual( record, createExistingRecord( false, false ) );
        }
    }

    @Test
    public void mustRetryInconsistentLightReads()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            nextReadIsInconsistent.set( true );
            R record = getLight( ID, store );
            assertRecordsEqual( record, createExistingRecord( false, true ) );
        }
    }

    @Test
    public void mustRetryInconsistentForcedReads()
    {
        try ( NeoStores neoStores = storeFixture() )
        {
            S store = getStore( neoStores );
            nextReadIsInconsistent.set( true );
            R record = getForce( store, ID );
            assertRecordsEqual( record, createExistingRecord( true, false ) );
        }
    }

    public static class RelationshipStoreConsistentReadTest extends RecordStoreConsistentReadTest<RelationshipRecord, RelationshipStore>
    {
        // Constants for the contents of the existing record
        private static final int FIRST_NODE = 2;
        private static final int SECOND_NODE = 3;
        private static final int TYPE = 4;
        private static final int FIRST_PREV_REL = 5;
        private static final int FIRST_NEXT_REL = 6;
        private static final int SECOND_PREV_REL = 7;
        private static final int SECOND_NEXT_REL = 8;

        @Override
        protected RelationshipRecord createNullRecord( long id )
        {
            RelationshipRecord record = new RelationshipRecord( id, false, 0, 0, 0, 0, 0, 0, 0, false, false );
            record.setNextProp( 0 );
            return record;
        }

        @Override
        protected RelationshipRecord createExistingRecord( boolean forced, boolean light )
        {
            return new RelationshipRecord(
                    ID, true, FIRST_NODE, SECOND_NODE, TYPE, FIRST_PREV_REL,
                    FIRST_NEXT_REL, SECOND_PREV_REL, SECOND_NEXT_REL, true, true );
        }

        @Override
        protected RelationshipRecord getLight( long id, RelationshipStore store )
        {
            return store.getLightRel( id );
        }

        @Override
        protected void assertRecordsEqual( RelationshipRecord actualRecord, RelationshipRecord expectedRecord )
        {
            assertNotNull( "actualRecord", actualRecord );
            assertNotNull( "expectedRecord", expectedRecord );
            assertThat( "getFirstNextRel", actualRecord.getFirstNextRel(), is( expectedRecord.getFirstNextRel() ) );
            assertThat( "getFirstNode", actualRecord.getFirstNode(), is( expectedRecord.getFirstNode() ) );
            assertThat( "getFirstPrevRel", actualRecord.getFirstPrevRel(), is( expectedRecord.getFirstPrevRel() ) );
            assertThat( "getSecondNextRel", actualRecord.getSecondNextRel(), is( expectedRecord.getSecondNextRel() ) );
            assertThat( "getSecondNode", actualRecord.getSecondNode(), is( expectedRecord.getSecondNode() ) );
            assertThat( "getSecondPrevRel", actualRecord.getSecondPrevRel(), is( expectedRecord.getSecondPrevRel() ) );
            assertThat( "getType", actualRecord.getType(), is( expectedRecord.getType() ) );
            assertThat( "isFirstInFirstChain", actualRecord.isFirstInFirstChain(), is( expectedRecord.isFirstInFirstChain() ) );
            assertThat( "isFirstInSecondChain", actualRecord.isFirstInSecondChain(), is( expectedRecord.isFirstInSecondChain() ) );
            assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
            assertThat( "getLongId", actualRecord.getLongId(), is( expectedRecord.getLongId() ) );
            assertThat( "getNextProp", actualRecord.getNextProp(), is( expectedRecord.getNextProp() ) );
            assertThat( "inUse", actualRecord.inUse(), is( expectedRecord.inUse() ) );
        }

        @Override
        protected RelationshipStore getStore( NeoStores neoStores )
        {
            return neoStores.getRelationshipStore();
        }
    }

    public static class LabelTokenStoreConsistentReadTest extends RecordStoreConsistentReadTest<LabelTokenRecord, LabelTokenStore>
    {

        private static final int NAME_RECORD_ID = 2;
        private static final byte[] NAME_RECORD_DATA = "TheLabel".getBytes( Charset.forName( "UTF-8" ) );

        @Override
        protected LabelTokenStore getStore( NeoStores neoStores )
        {
            return neoStores.getLabelTokenStore();
        }

        @Override
        protected LabelTokenStore initialiseStore( NeoStores neoStores )
        {
            LabelTokenStore store = getStore( neoStores );
            LabelTokenRecord record = createExistingRecord( false, false );
            DynamicRecord nameRecord = new DynamicRecord( NAME_RECORD_ID );
            record.getNameRecords().clear();
            nameRecord.setData( NAME_RECORD_DATA );
            nameRecord.setInUse( true );
            record.addNameRecord( nameRecord );
            store.updateRecord( record );
            return store;
        }

        @Override
        protected LabelTokenRecord createNullRecord( long id )
        {
            LabelTokenRecord labelTokenRecord = new LabelTokenRecord( (int) id );
            labelTokenRecord.setIsLight( true );
            return labelTokenRecord;
        }

        @Override
        protected LabelTokenRecord createExistingRecord( boolean forced, boolean light )
        {
            LabelTokenRecord record = new LabelTokenRecord( ID );
            record.setNameId( NAME_RECORD_ID );
            record.setInUse( true );
            record.setIsLight( forced );
            if ( !forced )
            {
                DynamicRecord nameRecord = new DynamicRecord( NAME_RECORD_ID );
                nameRecord.setLength( NAME_RECORD_DATA.length );
                nameRecord.setInUse( true );
                record.addNameRecord( nameRecord );
            }
            return record;
        }

        @Override
        protected LabelTokenRecord getLight( long id, LabelTokenStore store )
        {
            throw new AssumptionViolatedException( "No light loading of LabelTokenRecords" );
        }

        @Override
        protected void assertRecordsEqual( LabelTokenRecord actualRecord, LabelTokenRecord expectedRecord )
        {
            assertNotNull( "actualRecord", actualRecord );
            assertNotNull( "expectedRecord", expectedRecord );
            assertThat( "getNameId", actualRecord.getNameId(), is( expectedRecord.getNameId() ) );
            assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
            assertThat( "getLongId", actualRecord.getLongId(), is( expectedRecord.getLongId() ) );
            assertThat( "isLight", actualRecord.isLight(), is( expectedRecord.isLight() ) );

            Collection<DynamicRecord> actualNameRecords = actualRecord.getNameRecords();
            Collection<DynamicRecord> expectedNameRecords = expectedRecord.getNameRecords();
            assertThat( "getNameRecords.size", actualNameRecords.size(), is( expectedNameRecords.size() ) );
            Iterator<DynamicRecord> actualNRs = actualNameRecords.iterator();
            Iterator<DynamicRecord> expectedNRs = expectedNameRecords.iterator();
            int i = 0;
            while ( actualNRs.hasNext() && expectedNRs.hasNext() )
            {
                DynamicRecord actualNameRecord = actualNRs.next();
                DynamicRecord expectedNameRecord = expectedNRs.next();

                assertThat( "[" + i + "]getData", actualNameRecord.getData(), is( expectedNameRecord.getData() ) );
                assertThat( "[" + i + "]getLength", actualNameRecord.getLength(), is( expectedNameRecord.getLength() ) );
                assertThat( "[" + i + "]getNextBlock", actualNameRecord.getNextBlock(), is( expectedNameRecord.getNextBlock() ) );
                assertThat( "[" + i + "]getType", actualNameRecord.getType(), is( expectedNameRecord.getType() ) );
                assertThat( "[" + i + "]getId", actualNameRecord.getId(), is( expectedNameRecord.getId() ) );
                assertThat( "[" + i + "]getLongId", actualNameRecord.getLongId(), is( expectedNameRecord.getLongId() ) );
                assertThat( "[" + i + "]isLight", actualNameRecord.isLight(), is( expectedNameRecord.isLight() ) );
                assertThat( "[" + i + "]isStartRecord", actualNameRecord.isStartRecord(), is( expectedNameRecord.isStartRecord() ) );
                assertThat( "[" + i + "]inUse", actualNameRecord.inUse(), is( expectedNameRecord.inUse() ) );
                i++;
            }
        }
    }

    // This one might be good enough to cover all AbstractDynamicStore subclasses,
    // including DynamicArrayStore and DynamicStringStore.
    public static class SchemaStoreConsistentReadTest
            extends RecordStoreConsistentReadTest<DynamicRecord, SchemaStore>
    {
        private static final byte[] EXISTING_RECORD_DATA = "Random bytes".getBytes();

        @Override
        protected SchemaStore getStore( NeoStores neoStores )
        {
            return neoStores.getSchemaStore();
        }

        @Override
        protected DynamicRecord createNullRecord( long id )
        {
            DynamicRecord record = new DynamicRecord( id );
            record.setNextBlock( 0 );
            return record;
        }

        @Override
        protected DynamicRecord createExistingRecord( boolean forced, boolean light )
        {
            DynamicRecord record = new DynamicRecord( ID );
            record.setInUse( true );
            record.setStartRecord( true );
            record.setLength( EXISTING_RECORD_DATA.length );
            if ( !light )
            {
                record.setData( EXISTING_RECORD_DATA );
            }
            return record;
        }

        @Override
        protected DynamicRecord getLight( long id, SchemaStore store )
        {
            throw new AssumptionViolatedException( "Light loading of DynamicRecords is a little different" );
        }

        @Override
        protected void assertRecordsEqual( DynamicRecord actualRecord, DynamicRecord expectedRecord )
        {
            assertNotNull( "actualRecord", actualRecord );
            assertNotNull( "expectedRecord", expectedRecord );
            assertThat( "getData", actualRecord.getData(), is( expectedRecord.getData() ) );
            assertThat( "getLength", actualRecord.getLength(), is( expectedRecord.getLength() ) );
            assertThat( "getNextBlock", actualRecord.getNextBlock(), is( expectedRecord.getNextBlock() ) );
            assertThat( "getType", actualRecord.getType(), is( expectedRecord.getType() ) );
            assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
            assertThat( "getLongId", actualRecord.getLongId(), is( expectedRecord.getLongId() ) );
            assertThat( "isLight", actualRecord.isLight(), is( expectedRecord.isLight() ) );
            assertThat( "isStartRecord", actualRecord.isStartRecord(), is( expectedRecord.isStartRecord() ) );
        }
    }

    public static class PropertyStoreConsistentReadTest
            extends RecordStoreConsistentReadTest<PropertyRecord, PropertyStore>
    {
        @Override
        protected PropertyStore getStore( NeoStores neoStores )
        {
            return neoStores.getPropertyStore();
        }

        @Override
        protected PropertyRecord createNullRecord( long id )
        {
            PropertyRecord record = new PropertyRecord( id );
            record.setNextProp( 0 );
            record.setPrevProp( 0 );
            return record;
        }

        @Override
        protected PropertyRecord createExistingRecord( boolean forced, boolean light )
        {
            PropertyRecord record = new PropertyRecord( ID );
            record.setId( ID );
            record.setNextProp( 2 );
            record.setPrevProp( 4 );
            record.setInUse( true );
            PropertyBlock block = new PropertyBlock();
            DynamicRecordAllocator stringAllocator = new DynamicRecordAllocator()
            {
                @Override
                public int dataSize()
                {
                    return 64;
                }

                @Override
                public DynamicRecord nextUsedRecordOrNew( Iterator<DynamicRecord> recordsToUseFirst )
                {
                    DynamicRecord record = new DynamicRecord( 7 );
                    record.setCreated();
                    record.setInUse( true );
                    return record;
                }
            };
            String value = "a string too large to fit in the property block itself";
            PropertyStore.encodeValue( block, 6, value, stringAllocator, null );
            if ( forced  || light )
            {
                block.getValueRecords().clear();
            }
            record.setPropertyBlock( block );
            return record;
        }

        @Override
        protected PropertyRecord getLight( long id, PropertyStore store )
        {
            throw new AssumptionViolatedException( "Getting a light non-existing property record will throw." );
        }

        @Override
        protected PropertyRecord getHeavy( PropertyStore store, int id )
        {
            PropertyRecord record = super.getHeavy( store, id );
            ensureHeavy( store, record );
            return record;
        }

        private void ensureHeavy( PropertyStore store, PropertyRecord record )
        {
            for ( PropertyBlock propertyBlock : record )
            {
                store.ensureHeavy( propertyBlock );
            }
        }

        @Override
        protected void assertRecordsEqual( PropertyRecord actualRecord, PropertyRecord expectedRecord )
        {
            assertNotNull( "actualRecord", actualRecord );
            assertNotNull( "expectedRecord", expectedRecord );
            assertThat( "getDeletedRecords", actualRecord.getDeletedRecords(), is( expectedRecord.getDeletedRecords() ) );
            assertThat( "getNextProp", actualRecord.getNextProp(), is( expectedRecord.getNextProp() ) );
            assertThat( "getNodeId", actualRecord.getNodeId(), is( expectedRecord.getNodeId() ) );
            assertThat( "getPrevProp", actualRecord.getPrevProp(), is( expectedRecord.getPrevProp() ) );
            assertThat( "getRelId", actualRecord.getRelId(), is( expectedRecord.getRelId() ) );
            assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
            assertThat( "getLongId", actualRecord.getLongId(), is( expectedRecord.getLongId() ) );

            List<PropertyBlock> actualBlocks = asList( (Iterable<PropertyBlock>) actualRecord );
            List<PropertyBlock> expectedBlocks = asList( (Iterable<PropertyBlock>) expectedRecord );
            assertThat( "getPropertyBlocks().size", actualBlocks.size(), is( expectedBlocks.size() ) );
            for ( int i = 0; i < actualBlocks.size(); i++ )
            {
                PropertyBlock actualBlock = actualBlocks.get( i );
                PropertyBlock expectedBlock = expectedBlocks.get( i );
                assertPropertyBlocksEqual( i, actualBlock, expectedBlock );
            }
        }

        private void assertPropertyBlocksEqual( int index, PropertyBlock actualBlock, PropertyBlock expectedBlock )
        {
            assertThat( "[" + index + "]getKeyIndexId", actualBlock.getKeyIndexId(),
                    is( expectedBlock.getKeyIndexId() ) );
            assertThat( "[" + index + "]getSingleValueBlock", actualBlock.getSingleValueBlock(), is( expectedBlock.getSingleValueBlock() ) );
            assertThat( "[" + index + "]getSingleValueByte", actualBlock.getSingleValueByte(), is( expectedBlock.getSingleValueByte() ) );
            assertThat( "[" + index + "]getSingleValueInt", actualBlock.getSingleValueInt(), is( expectedBlock.getSingleValueInt() ) );
            assertThat( "[" + index + "]getSingleValueLong", actualBlock.getSingleValueLong(), is( expectedBlock.getSingleValueLong() ) );
            assertThat( "[" + index + "]getSingleValueShort", actualBlock.getSingleValueShort(), is( expectedBlock.getSingleValueShort() ) );
            assertThat( "[" + index + "]getSize", actualBlock.getSize(), is( expectedBlock.getSize() ) );
            assertThat( "[" + index + "]getType", actualBlock.getType(), is( expectedBlock.getType() ) );
            assertThat( "[" + index + "]isLight", actualBlock.isLight(), is( expectedBlock.isLight() ) );

            List<DynamicRecord> actualValueRecords = actualBlock.getValueRecords();
            List<DynamicRecord> expectedValueRecords = expectedBlock.getValueRecords();
            assertThat( "[" + index + "]getValueRecords.size",
                    actualValueRecords.size(), is( expectedValueRecords.size() ) );

            for ( int i = 0; i < actualValueRecords.size(); i++ )
            {
                DynamicRecord actualValueRecord = actualValueRecords.get( i );
                DynamicRecord expectedValueRecord = expectedValueRecords.get( i );
                assertThat( "[" + index + "]getValueRecords[" + i + "]getData", actualValueRecord.getData(), is( expectedValueRecord.getData() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]getLength", actualValueRecord.getLength(), is( expectedValueRecord.getLength() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]getNextBlock", actualValueRecord.getNextBlock(), is( expectedValueRecord.getNextBlock() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]getType", actualValueRecord.getType(), is( expectedValueRecord.getType() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]getId", actualValueRecord.getId(), is( expectedValueRecord.getId() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]getLongId", actualValueRecord.getLongId(), is( expectedValueRecord.getLongId() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]isLight", actualValueRecord.isLight(), is( expectedValueRecord.isLight() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]isStartRecord", actualValueRecord.isStartRecord(), is( expectedValueRecord.isStartRecord() ) );
                assertThat( "[" + index + "]getValueRecords[" + i + "]inUse", actualValueRecord.inUse(), is( expectedValueRecord.inUse() ) );
            }
        }
    }
}
