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
package org.neo4j.kernel.impl.storemigration.legacystore.v20;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;

public class Legacy20RelationshipStoreReader implements LegacyRelationshipStoreReader

{
    public static final String FROM_VERSION = "RelationshipStore " + Legacy20Store.LEGACY_VERSION;
    public static final int RECORD_SIZE = 33;

    private final StoreChannel fileChannel;
    private final long maxId;

    public Legacy20RelationshipStoreReader( FileSystemAbstraction fs, File fileName ) throws IOException
    {
        fileChannel = fs.open( fileName, "r" );
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_SIZE;
    }

    @Override
    public long getMaxId()
    {
        return maxId;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    /**
     * @param approximateStartId the scan will start at the beginning of the page this id is located in.
     */
    @Override
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
            long firstNode = Legacy20Store.getUnsignedInt( buffer );
            long firstNodeMod = (inUseByte & 0xEL) << 31;

            long secondNode = Legacy20Store.getUnsignedInt( buffer );

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            long typeInt = buffer.getInt();
            long secondNodeMod = (typeInt & 0x70000000L) << 4;
            int type = (int) (typeInt & 0xFFFF);

            firstNode = Legacy20Store.longFromIntAndMod( firstNode, firstNodeMod );
            secondNode = Legacy20Store.longFromIntAndMod( secondNode, secondNodeMod );

            long firstPrevRel = Legacy20Store.getUnsignedInt( buffer );
            long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
            firstPrevRel =  Legacy20Store.longFromIntAndMod( firstPrevRel, firstPrevRelMod );

            long firstNextRel = Legacy20Store.getUnsignedInt( buffer );
            long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
            firstNextRel = Legacy20Store.longFromIntAndMod( firstNextRel, firstNextRelMod );

            long secondPrevRel = Legacy20Store.getUnsignedInt( buffer );
            long secondPrevRelMod = (typeInt & 0x380000L) << 13;
            secondPrevRel = Legacy20Store.longFromIntAndMod( secondPrevRel, secondPrevRelMod );

            long secondNextRel = Legacy20Store.getUnsignedInt( buffer );
            long secondNextRelMod = (typeInt & 0x70000L) << 16;
            secondNextRel = Legacy20Store.longFromIntAndMod( secondNextRel, secondNextRelMod );

            long nextProp = Legacy20Store.getUnsignedInt( buffer );
            long nextPropMod = (inUseByte & 0xF0L) << 28;
            nextProp = Legacy20Store.longFromIntAndMod( nextProp, nextPropMod );

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
