/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore.longFromIntAndMod;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class LegacyNodeStoreReader
{
    public static final String FROM_VERSION = "NodeStore v0.9.9";
    public static final int RECORD_LENGTH = 9;

    private final FileChannel fileChannel;
    private final long maxId;

    public LegacyNodeStoreReader( String fileName ) throws IOException
    {
        fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_LENGTH;
    }

    public long getMaxId()
    {
        return maxId;
    }

    public Iterable<NodeRecord> readNodeStore() throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect( RECORD_LENGTH );

        return new Iterable<NodeRecord>()
        {
            @Override
            public Iterator<NodeRecord> iterator()
            {
                return new PrefetchingIterator<NodeRecord>()
                {
                    long id = 0;

                    @Override
                    protected NodeRecord fetchNextOrNull()
                    {
                        NodeRecord nodeRecord = null;
                        while ( nodeRecord == null && id <= maxId )
                        {
                            buffer.clear();
                            try
                            {
                                fileChannel.read( buffer );
                            } catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                            buffer.flip();
                            long inUseByte = buffer.get();

                            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
                            if ( inUse )
                            {
                                long nextRel = LegacyStore.getUnsignedInt( buffer );
                                long relModifier = (inUseByte & 0xEL) << 31;
                                long nextProp = LegacyStore.getUnsignedInt( buffer );
                                long propModifier = (inUseByte & 0xF0L) << 28;
                                nodeRecord = new NodeRecord( id, longFromIntAndMod( nextRel, relModifier ), longFromIntAndMod( nextProp, propModifier ) );
                            }
                            else nodeRecord = new NodeRecord( id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() );
                            nodeRecord.setInUse( inUse );
                            id++;
                        }
                        return nodeRecord;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public void close() throws IOException
    {
        fileChannel.close();
    }
}
