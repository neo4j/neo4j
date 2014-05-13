/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

public class LegacyRelationshipStoreReader implements Closeable
{
    public static class ReusableRelationship
    {
        private long recordId;
        private boolean inUse;
        private long firstNode;
        private long secondNode;
        private int type;
        private long firstPrevRel;
        private long firstNextRel;
        private long secondNextRel;
        private long secondPrevRel;
        private long nextProp;

        private RelationshipRecord record;

        public void reset(long id, boolean inUse, long firstNode, long secondNode, int type, long firstPrevRel,
                          long firstNextRel, long secondNextRel, long secondPrevRel, long nextProp)
        {
            this.record = null;
            this.recordId = id;
            this.inUse = inUse;
            this.firstNode = firstNode;
            this.secondNode = secondNode;
            this.type = type;
            this.firstPrevRel = firstPrevRel;
            this.firstNextRel = firstNextRel;
            this.secondNextRel = secondNextRel;
            this.secondPrevRel = secondPrevRel;
            this.nextProp = nextProp;
        }

        public boolean inUse()
        {
            return inUse;
        }

        public long getFirstNode()
        {
            return firstNode;
        }

        public long getFirstNextRel()
        {
            return firstNextRel;
        }

        public long getSecondNode()
        {
            return secondNode;
        }

        public long getFirstPrevRel()
        {
            return firstPrevRel;
        }

        public long getSecondPrevRel()
        {
            return secondPrevRel;
        }

        public long getSecondNextRel()
        {
            return secondNextRel;
        }

        public long id()
        {
            return recordId;
        }

        public RelationshipRecord createRecord()
        {
            if( record == null)
            {
                record = new RelationshipRecord( recordId, firstNode, secondNode, type );
                record.setInUse( inUse );
                record.setFirstPrevRel( firstPrevRel );
                record.setFirstNextRel( firstNextRel );
                record.setSecondPrevRel( secondPrevRel );
                record.setSecondNextRel( secondNextRel );
                record.setNextProp( nextProp );

            }
            return record;
        }
    }

    public static final String FROM_VERSION = "RelationshipStore " + LegacyStore.LEGACY_VERSION;
    public static final int RECORD_SIZE = 33;

    private final StoreChannel fileChannel;
    private final long maxId;

    public LegacyRelationshipStoreReader( FileSystemAbstraction fs, File fileName ) throws IOException
    {
        fileChannel = fs.open( fileName, "r" );
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_SIZE;
    }

    public long getMaxId()
    {
        return maxId;
    }

    /**
     * @param approximateStartId the scan will start at the beginning of the page this id is located in.
     */
    public void accept( long approximateStartId, Visitor<ReusableRelationship, RuntimeException> visitor ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( 4 * 1024 * RECORD_SIZE );
        ReusableRelationship rel = new ReusableRelationship();

        long position = (approximateStartId * RECORD_SIZE) - ( (approximateStartId * RECORD_SIZE) % buffer.capacity()),
             fileSize = fileChannel.size();

        while(position < fileSize)
        {
            int recordOffset = 0;
            buffer.clear();
            fileChannel.read( buffer, position );
            // Visit each record in the page
            while(recordOffset < buffer.capacity() && (recordOffset + position) < fileSize)
            {
                buffer.position(recordOffset);
                long id = (position + recordOffset) / RECORD_SIZE;

                readRecord(buffer, id, rel);

                if(visitor.visit( rel ))
                {
                    return;
                }

                recordOffset += RECORD_SIZE;
            }

            position += buffer.capacity();
        }
    }

    public Iterator<RelationshipRecord> iterator( final long approximateStartId ) throws IOException
    {
        final ReusableRelationship rel = new ReusableRelationship();
        final ByteBuffer buffer = ByteBuffer.allocateDirect( 4 * 1024 * RECORD_SIZE );
        final long fileSize = fileChannel.size();
        return new PrefetchingIterator<RelationshipRecord>()
        {
            private long position = (approximateStartId * RECORD_SIZE) - ( (approximateStartId * RECORD_SIZE) % buffer.capacity());
            private final Collection<RelationshipRecord> pageRecords = new ArrayList<>();
            private Iterator<RelationshipRecord> pageRecordsIterator = IteratorUtil.emptyIterator();

            @Override
            protected RelationshipRecord fetchNextOrNull()
            {
                // Next from current page
                if ( pageRecordsIterator.hasNext() )
                {
                    return pageRecordsIterator.next();
                }

                while ( position < fileSize )
                {
                    int recordOffset = 0;
                    buffer.clear();
                    try
                    {
                        fileChannel.read( buffer, position );
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                    // Visit each record in the page
                    pageRecords.clear();
                    while(recordOffset < buffer.capacity() && (recordOffset + position) < fileSize)
                    {
                        buffer.position(recordOffset);
                        long id = (position + recordOffset) / RECORD_SIZE;

                        readRecord( buffer, id, rel );

                        if ( rel.inUse() )
                        {
                            pageRecords.add( rel.createRecord() );
                        }

                        recordOffset += RECORD_SIZE;
                    }

                    position += buffer.capacity();
                    pageRecordsIterator = pageRecords.iterator();
                    if ( pageRecordsIterator.hasNext() )
                    {
                        return pageRecordsIterator.next();
                    }
                }
                return null;
            }
        };
    }

    private void readRecord( ByteBuffer buffer, long id, ReusableRelationship rel)
    {
        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( inUse )
        {
            long firstNode = LegacyStore.getUnsignedInt( buffer );
            long firstNodeMod = (inUseByte & 0xEL) << 31;

            long secondNode = LegacyStore.getUnsignedInt( buffer );

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            long typeInt = buffer.getInt();
            long secondNodeMod = (typeInt & 0x70000000L) << 4;
            int type = (int) (typeInt & 0xFFFF);

            firstNode = LegacyStore.longFromIntAndMod( firstNode, firstNodeMod );
            secondNode = LegacyStore.longFromIntAndMod( secondNode, secondNodeMod );

            long firstPrevRel = LegacyStore.getUnsignedInt( buffer );
            long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
            firstPrevRel =  LegacyStore.longFromIntAndMod( firstPrevRel, firstPrevRelMod );

            long firstNextRel = LegacyStore.getUnsignedInt( buffer );
            long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
            firstNextRel = LegacyStore.longFromIntAndMod( firstNextRel, firstNextRelMod );

            long secondPrevRel = LegacyStore.getUnsignedInt( buffer );
            long secondPrevRelMod = (typeInt & 0x380000L) << 13;
            secondPrevRel = LegacyStore.longFromIntAndMod( secondPrevRel, secondPrevRelMod );

            long secondNextRel = LegacyStore.getUnsignedInt( buffer );
            long secondNextRelMod = (typeInt & 0x70000L) << 16;
            secondNextRel = LegacyStore.longFromIntAndMod( secondNextRel, secondNextRelMod );

            long nextProp = LegacyStore.getUnsignedInt( buffer );
            long nextPropMod = (inUseByte & 0xF0L) << 28;
            nextProp = LegacyStore.longFromIntAndMod( nextProp, nextPropMod );

            rel.reset( id, true, firstNode, secondNode, type,
                       firstPrevRel, firstNextRel, secondNextRel, secondPrevRel, nextProp );
        }
        else
        {
            rel.reset( id, false, -1, -1, -1, -1, -1, -1, -1, -1 );
        }
    }

    @Override
    public void close() throws IOException
    {
        fileChannel.close();
    }
}
