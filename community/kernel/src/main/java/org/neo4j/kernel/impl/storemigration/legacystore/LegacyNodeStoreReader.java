/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

import static java.nio.ByteBuffer.allocateDirect;

import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore.longFromIntAndMod;
import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore.readIntoBuffer;

public class LegacyNodeStoreReader implements Closeable
{
    public static final String FROM_VERSION = "NodeStore " + LegacyStore.LEGACY_VERSION;
    public static final int RECORD_SIZE = 9;

    private final StoreChannel fileChannel;
    private final long maxId;

    public LegacyNodeStoreReader( FileSystemAbstraction fs, File fileName ) throws IOException
    {
        fileChannel = fs.open( fileName, "r" );
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_SIZE;
    }

    public long getMaxId()
    {
        return maxId;
    }

    public Iterator<NodeRecord> readNodeStore() throws IOException
    {
        return new PrefetchingIterator<NodeRecord>()
        {
            long id = 0;
            ByteBuffer buffer = allocateDirect( RECORD_SIZE );

            @Override
            protected NodeRecord fetchNextOrNull()
            {
                NodeRecord nodeRecord = null;
                while ( nodeRecord == null && id <= maxId )
                {
                    readIntoBuffer( fileChannel, buffer, RECORD_SIZE );
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

    @Override
    public void close() throws IOException
    {
        fileChannel.close();
    }
}
