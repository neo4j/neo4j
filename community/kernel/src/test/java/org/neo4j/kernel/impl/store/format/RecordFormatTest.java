/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.format.current.Current;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.test.RandomRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.format.current.DynamicRecordFormat.RECORD_HEADER_SIZE;

@RunWith( Parameterized.class )
public class RecordFormatTest
{
    //==========================================================================
    //========= RULES AND CONSTANTS ============================================
    //==========================================================================

    private static final int TEST_ITERATIONS = 20_000;
    private static final int _16B = 1 << 16;
    private static final int _32B = 1 << 32;
    private static final long _35B = 1L << 35;
    private static final long _36B = 1L << 36;
    private static final long _40B = 1L << 40;
    private static final long NULL = -1;
    private static final int BLOCK_SIZE = 120;
    // This relies on record header size of a particular format, works for now, but should be changed.
    private static final int DATA_SIZE = BLOCK_SIZE - RECORD_HEADER_SIZE;

    @Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();
        data.add( array( Current.RECORD_FORMATS, LOW_LIMITS ) );
        return data;
    }

    @ClassRule
    public static final RandomRule random = new RandomRule();
    @Parameter( 0 )
    public RecordFormats formats;
    @Parameter( 1 )
    public RecordKeys keyFactory;

    //==========================================================================
    //========= FORMATS THAT MAKES UP THE TEST SPECS ===========================
    //==========================================================================

    private static long randomLong( long max )
    {
        return randomLong( max, NULL );
    }

    private static long randomLong( long max, long nullValue )
    {
        return random.nextFloat() < 0.2 ? nullValue : random.nextLong( max );
    }

    private static final RecordKeys LOW_LIMITS = new RecordKeys()
    {
        @Override
        public RecordKey<NodeRecord> node()
        {
            return new AbstractRecordKey<NodeRecord>()
            {
                @Override
                public NodeRecord get()
                {
                    return new NodeRecord( 0 ).initialize(
                            random.nextBoolean(), randomLong( _35B ), random.nextBoolean(), randomLong( _35B ),
                            randomLong( _40B, 0 ) );
                }

                @Override
                public void assertRecordsEquals( NodeRecord written, NodeRecord read )
                {
                    assertEquals( written.getNextProp(), read.getNextProp() );
                    assertEquals( written.getNextRel(), read.getNextRel() );
                    assertEquals( written.getLabelField(), read.getLabelField() );
                    assertEquals( written.isDense(), read.isDense() );
                }
            };
        }

        @Override
        public RecordKey<RelationshipRecord> relationship()
        {
            return new AbstractRecordKey<RelationshipRecord>()
            {
                @Override
                public RelationshipRecord get()
                {
                    return new RelationshipRecord( 0 ).initialize( random.nextBoolean(), randomLong( _36B ),
                            random.nextLong( _35B ), random.nextLong( _35B ), random.nextInt( _16B ),
                            randomLong( _35B ), randomLong( _35B ),
                            randomLong( _35B ), randomLong( _35B ),
                            random.nextBoolean(), random.nextBoolean() );
                }

                @Override
                public void assertRecordsEquals( RelationshipRecord written, RelationshipRecord read )
                {
                    assertEquals( written.getNextProp(), read.getNextProp() );
                    assertEquals( written.getFirstNode(), read.getFirstNode() );
                    assertEquals( written.getSecondNode(), read.getSecondNode() );
                    assertEquals( written.getType(), read.getType() );
                    assertEquals( written.getFirstPrevRel(), read.getFirstPrevRel() );
                    assertEquals( written.getFirstNextRel(), read.getFirstNextRel() );
                    assertEquals( written.getSecondPrevRel(), read.getSecondPrevRel() );
                    assertEquals( written.getSecondNextRel(), read.getSecondNextRel() );
                }
            };
        }

        @Override
        public RecordKey<PropertyRecord> property()
        {
            return new AbstractRecordKey<PropertyRecord>()
            {
                @Override
                public PropertyRecord get()
                {
                    PropertyRecord record = new PropertyRecord( 0 );
                    int blocks = random.intBetween( 1, 4 );
                    MyDynamicRecordAllocator stringAllocator = new MyDynamicRecordAllocator();
                    MyDynamicRecordAllocator arrayAllocator = new MyDynamicRecordAllocator();
                    for ( int i = 0; i < blocks; i++ )
                    {
                        PropertyBlock block = new PropertyBlock();
                        // Dynamic records will not be written and read by the property record format,
                        // that happens in the store where it delegates to a "sub" store.
                        PropertyStore.encodeValue( block, random.nextInt( _16B ), random.propertyValue(),
                                stringAllocator, arrayAllocator );
                    }
                    return record;
                }

                @Override
                public void assertRecordsEquals( PropertyRecord written, PropertyRecord read )
                {
                    assertEquals( written.getNextProp(), read.getNextProp() );
                    assertEquals( written.isNodeSet(), read.isNodeSet() );
                    if ( written.isNodeSet() )
                    {
                        assertEquals( written.getNodeId(), read.getNodeId() );
                    }
                    else
                    {
                        assertEquals( written.getRelId(), read.getRelId() );
                    }
                    assertEquals( written.numberOfProperties(), read.numberOfProperties() );
                    Iterator<PropertyBlock> writtenBlocks = written.iterator();
                    Iterator<PropertyBlock> readBlocks = read.iterator();
                    while ( writtenBlocks.hasNext() )
                    {
                        assertTrue( readBlocks.hasNext() );
                        assertBlocksEquals( writtenBlocks.next(), readBlocks.next() );
                    }
                }

                private void assertBlocksEquals( PropertyBlock written, PropertyBlock read )
                {
                    assertEquals( written.getKeyIndexId(), read.getKeyIndexId() );
                    assertEquals( written.getSize(), read.getSize() );
                    assertTrue( written.hasSameContentsAs( read ) );
                    assertArrayEquals( written.getValueBlocks(), read.getValueBlocks() );
                }
            };
        }

        @Override
        public RecordKey<RelationshipGroupRecord> relationshipGroup()
        {
            return new AbstractRecordKey<RelationshipGroupRecord>()
            {
                @Override
                public RelationshipGroupRecord get()
                {
                    return new RelationshipGroupRecord( 0 ).initialize( random.nextBoolean(),
                            random.nextInt( _16B ), randomLong( _35B ), randomLong( _35B ),
                            randomLong( _35B ), randomLong( _35B ), randomLong( _35B ) );
                }

                @Override
                public void assertRecordsEquals( RelationshipGroupRecord written, RelationshipGroupRecord read )
                {
                    assertEquals( written.getType(), read.getType() );
                    assertEquals( written.getFirstOut(), read.getFirstOut() );
                    assertEquals( written.getFirstIn(), read.getFirstIn() );
                    assertEquals( written.getFirstLoop(), read.getFirstLoop() );
                    assertEquals( written.getNext(), read.getNext() );
                    assertEquals( written.getOwningNode(), read.getOwningNode() );
                }
            };
        }

        @Override
        public RecordKey<RelationshipTypeTokenRecord> relationshipTypeToken()
        {
            return new AbstractRecordKey<RelationshipTypeTokenRecord>()
            {
                @Override
                public RelationshipTypeTokenRecord get()
                {
                    return new RelationshipTypeTokenRecord( 0 ).initialize( random.nextBoolean(),
                            random.nextInt( _16B ) );
                }

                @Override
                public void assertRecordsEquals( RelationshipTypeTokenRecord written, RelationshipTypeTokenRecord read )
                {
                    assertEquals( written.getNameId(), read.getNameId() );
                }
            };
        }

        @Override
        public RecordKey<PropertyKeyTokenRecord> propertyKeyToken()
        {
            return new AbstractRecordKey<PropertyKeyTokenRecord>()
            {
                @Override
                public PropertyKeyTokenRecord get()
                {
                    return new PropertyKeyTokenRecord( 0 ).initialize( random.nextBoolean(),
                            random.nextInt( _16B ), random.nextInt( _32B ) );
                }

                @Override
                public void assertRecordsEquals( PropertyKeyTokenRecord written, PropertyKeyTokenRecord read )
                {
                    assertEquals( written.getNameId(), read.getNameId() );
                    assertEquals( written.getPropertyCount(), read.getPropertyCount() );
                }
            };
        }

        @Override
        public RecordKey<LabelTokenRecord> labelToken()
        {
            return new AbstractRecordKey<LabelTokenRecord>()
            {
                @Override
                public LabelTokenRecord get()
                {
                    return new LabelTokenRecord( 0 ).initialize( random.nextBoolean(),
                            random.nextInt( _16B ) );
                }

                @Override
                public void assertRecordsEquals( LabelTokenRecord written, LabelTokenRecord read )
                {
                    assertEquals( written.getNameId(), read.getNameId() );
                }
            };
        }

        @Override
        public RecordKey<DynamicRecord> dynamic()
        {
            return new RecordKey<DynamicRecord>()
            {
                @Override
                public DynamicRecord get()
                {
                    int length = random.nextBoolean() ? DATA_SIZE : random.nextInt( DATA_SIZE );
                    long next = length == DATA_SIZE ? random.nextLong( _36B ) : NULL;
                    DynamicRecord record = new DynamicRecord( 1 ).initialize( random.nextBoolean(),
                            random.nextBoolean(), next, random.nextInt( PropertyType.values().length ), length );
                    byte[] data = new byte[record.getLength()];
                    random.nextBytes( data );
                    record.setData( data );
                    return record;
                }

                @Override
                public void assertRecordsEquals( DynamicRecord written, DynamicRecord read )
                {
                    // Don't assert type, since that's read from the data, and the data in this test
                    // is randomly generated. Since we assert that the data is the same then the type
                    // is also correct.
                    assertEquals( written.getLength(), read.getLength() );
                    assertEquals( written.getNextBlock(), read.getNextBlock() );
                    assertArrayEquals( written.getData(), read.getData() );
                    assertEquals( written.isStartRecord(), read.isStartRecord() );
                }

                @Override
                public StoreHeader storeHeader()
                {
                    return new IntStoreHeader( BLOCK_SIZE );
                }
            };
        }
    };

    //==========================================================================
    //========= THE ACTUAL TESTS ===============================================
    //==========================================================================

    @Test
    public void node() throws Exception
    {
        verifyWriteAndRead( formats::node, keyFactory::node );
    }

    @Test
    public void relationship() throws Exception
    {
        verifyWriteAndRead( formats::relationship, keyFactory::relationship );
    }

    @Test
    public void property() throws Exception
    {
        verifyWriteAndRead( formats::property, keyFactory::property );
    }

    @Test
    public void relationshipGroup() throws Exception
    {
        verifyWriteAndRead( formats::relationshipGroup, keyFactory::relationshipGroup );
    }

    @Test
    public void relationshipTypeToken() throws Exception
    {
        verifyWriteAndRead( formats::relationshipTypeToken, keyFactory::relationshipTypeToken );
    }

    @Test
    public void propertyKeyToken() throws Exception
    {
        verifyWriteAndRead( formats::propertyKeyToken, keyFactory::propertyKeyToken );
    }

    @Test
    public void labelToken() throws Exception
    {
        verifyWriteAndRead( formats::labelToken, keyFactory::labelToken );
    }

    @Test
    public void dynamic() throws Exception
    {
        verifyWriteAndRead( formats::dynamic, keyFactory::dynamic );
    }

    private <R extends AbstractBaseRecord> void verifyWriteAndRead( Supplier<RecordFormat<R>> formatSupplier,
            Supplier<RecordKey<R>> keyFactory )
    {
        // GIVEN
        RecordFormat<R> format = formatSupplier.get();
        PageCursor cursor = new StubPageCursor( 0, 1_000 );
        RecordKey<R> key = keyFactory.get();
        int recordSize = format.getRecordSize( key.storeHeader() );

        // WHEN
        for ( int i = 0; i < TEST_ITERATIONS; i++ )
        {
            R written = key.get();

            // write
            int offset = Math.toIntExact( written.getLongId() * recordSize );
            cursor.setOffset( offset );
            format.write( written, cursor );

            // read
            cursor.setOffset( offset );
            @SuppressWarnings( "unchecked" )
            R read = (R) written.clone(); // just to get a new instance
            format.read( read, cursor, RecordLoad.NORMAL, recordSize );

            // THEN
            if ( written.inUse() )
            {
                key.assertRecordsEquals( written, read );
            }
            else
            {
                assertEquals( written.inUse(), read.inUse() );
            }
        }
    }

    //==========================================================================
    //========= UTILITIES TO AID THE TESTING ===================================
    //==========================================================================

    interface RecordKeys
    {
        RecordKey<NodeRecord> node();

        RecordKey<RelationshipRecord> relationship();

        RecordKey<PropertyRecord> property();

        RecordKey<RelationshipGroupRecord> relationshipGroup();

        RecordKey<RelationshipTypeTokenRecord> relationshipTypeToken();

        RecordKey<PropertyKeyTokenRecord> propertyKeyToken();

        RecordKey<LabelTokenRecord> labelToken();

        RecordKey<DynamicRecord> dynamic();
    }

    interface RecordKey<RECORD extends AbstractBaseRecord> extends Supplier<RECORD>
    {
        void assertRecordsEquals( RECORD written, RECORD read );

        StoreHeader storeHeader();
    }

    abstract static class AbstractRecordKey<RECORD extends AbstractBaseRecord> implements RecordKey<RECORD>
    {
        @Override
        public StoreHeader storeHeader()
        {
            return NO_STORE_HEADER;
        }
    }

    protected static Collection<DynamicRecord> randomDynamicNodeLabelRecords()
    {
        Collection<DynamicRecord> records = new ArrayList<>();
        PropertyStore.allocateArrayRecords( records, randomLabelIds( 20 ), new MyDynamicRecordAllocator() );
        return records;
    }

    private static long[] randomLabelIds( int max )
    {
        long[] ids = new long[random.nextInt( max )];
        for ( int i = 0; i < ids.length; i++ )
        {
            ids[i] = random.nextInt( _16B );
        }
        return ids;
    }

    public static class MyDynamicRecordAllocator implements DynamicRecordAllocator
    {
        private int next = 1;

        @Override
        public int getRecordDataSize()
        {
            return 60;
        }

        @Override
        public DynamicRecord nextUsedRecordOrNew( Iterator<DynamicRecord> recordsToUseFirst )
        {
            return recordsToUseFirst.hasNext() ? recordsToUseFirst.next() : new DynamicRecord( next++ );
        }
    }
}
