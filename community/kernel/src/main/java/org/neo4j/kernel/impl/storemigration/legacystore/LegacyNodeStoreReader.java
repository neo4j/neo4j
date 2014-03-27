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
import java.util.Collections;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

import static java.nio.ByteBuffer.allocateDirect;

import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore.longFromIntAndMod;

public class LegacyNodeStoreReader implements Closeable
{
    public interface Visitor
    {
        void visit(NodeRecord record);
    }

    public static final String FROM_VERSION = "NodeStore " + LegacyStore.LEGACY_VERSION;
    public static final int RECORD_SIZE = 14;

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

    public void accept( Visitor visitor ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( 4 * 1024 * RECORD_SIZE );

        long position = 0, fileSize = fileChannel.size();
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

                visitor.visit( readRecord( buffer, id ) );

                recordOffset += RECORD_SIZE;
            }

            position += buffer.capacity()   ;
        }
    }

    private NodeRecord readRecord( ByteBuffer buffer, long id )
    {
        NodeRecord nodeRecord;

        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( inUse )
        {
            long nextRel = LegacyStore.getUnsignedInt( buffer );
            long relModifier = (inUseByte & 0xEL) << 31;
            long nextProp = LegacyStore.getUnsignedInt( buffer );
            long propModifier = (inUseByte & 0xF0L) << 28;
            long lsbLabels = LegacyStore.getUnsignedInt( buffer );
            long hsbLabels = buffer.get() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
            long labels = lsbLabels | (hsbLabels << 32);
            nodeRecord = new NodeRecord( id, false, longFromIntAndMod( nextRel, relModifier ),
                    longFromIntAndMod( nextProp, propModifier ) );
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

    public NodeRecord readNodeStore( long id ) throws IOException
    {
        ByteBuffer buffer = allocateDirect( RECORD_SIZE );
        NodeRecord nodeRecord;

        fileChannel.position( id * RECORD_SIZE );
        fileChannel.read( buffer );
        buffer.flip();

        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( inUse )
        {
            long nextRel = LegacyStore.getUnsignedInt( buffer );
            long relModifier = (inUseByte & 0xEL) << 31;
            long nextProp = LegacyStore.getUnsignedInt( buffer );
            long propModifier = (inUseByte & 0xF0L) << 28;
            long lsbLabels = LegacyStore.getUnsignedInt( buffer );
            long hsbLabels = buffer.get() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
            long labels = lsbLabels | (hsbLabels << 32);
            nodeRecord = new NodeRecord( id, false, longFromIntAndMod( nextRel, relModifier ),
                    longFromIntAndMod( nextProp, propModifier ) );
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
}
