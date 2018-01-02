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
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;

public class Legacy20NodeStoreReader implements LegacyNodeStoreReader
{
    public static final String FROM_VERSION = "NodeStore " + Legacy20Store.LEGACY_VERSION;
    public static final int RECORD_SIZE = 14;

    private final StoreChannel fileChannel;
    private final long maxId;

    public Legacy20NodeStoreReader( FileSystemAbstraction fs, File fileName ) throws IOException
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

    @Override
    public Iterator<NodeRecord> iterator() throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect( 4 * 1024 * RECORD_SIZE );
        final long fileSize = fileChannel.size();
        return new PrefetchingIterator<NodeRecord>()
        {
            private long position = 0;
            private final Collection<NodeRecord> pageRecords = new ArrayList<>();
            private Iterator<NodeRecord> pageRecordsIterator = IteratorUtil.emptyIterator();

            @Override
            protected NodeRecord fetchNextOrNull()
            {
                // Next from current page
                if ( pageRecordsIterator.hasNext() )
                {
                    return pageRecordsIterator.next();
                }

                // Read next page
                while ( position < fileSize )
                {
                    int pageOffset = 0;
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
                    while ( pageOffset < buffer.capacity() && (pageOffset + position) < fileSize )
                    {
                        buffer.position(pageOffset);
                        long id = (position + pageOffset) / RECORD_SIZE;

                        NodeRecord record = readRecord( buffer, id );
                        if ( record.inUse() )
                        {
                            pageRecords.add( record );
                        }

                        pageOffset += RECORD_SIZE;
                    }
                    position += buffer.capacity();
                    pageRecordsIterator = pageRecords.iterator();
                    if ( pageRecordsIterator.hasNext() )
                    {
                        return pageRecordsIterator.next();
                    }
                }

                // No more records
                return null;
            }
        };
    }

    private NodeRecord readRecord( ByteBuffer buffer, long id )
    {
        NodeRecord nodeRecord;

        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( inUse )
        {
            long nextRel = Legacy20Store.getUnsignedInt( buffer );
            long relModifier = (inUseByte & 0xEL) << 31;
            long nextProp = Legacy20Store.getUnsignedInt( buffer );
            long propModifier = (inUseByte & 0xF0L) << 28;
            long lsbLabels = Legacy20Store.getUnsignedInt( buffer );
            long hsbLabels = buffer.get() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
            long labels = lsbLabels | (hsbLabels << 32);
            nodeRecord = new NodeRecord( id, false, Legacy20Store.longFromIntAndMod( nextRel, relModifier ),
                    Legacy20Store.longFromIntAndMod( nextProp, propModifier ) );
            nodeRecord.setLabelField( labels, Collections.<DynamicRecord>emptyList() ); // no need to load 'em heavy
        }
        else
        {
            nodeRecord = new NodeRecord( id, false,
                    Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() );
        }
        nodeRecord.setInUse( inUse );

        return nodeRecord;
    }

    @Override
    public void close() throws IOException
    {
        fileChannel.close();
    }
}
