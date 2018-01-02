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
package org.neo4j.kernel.impl.storemigration.legacystore.v19;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

import static java.nio.ByteBuffer.allocateDirect;

import static org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store.getUnsignedInt;
import static org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store.longFromIntAndMod;
import static org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store.readIntoBuffer;

public class Legacy19PropertyStoreReader implements Closeable
{
    public static final String FROM_VERSION = "PropertyStore " + Legacy19Store.LEGACY_VERSION;
    public static final int RECORD_SIZE =
            1/*next and prev high bits*/ + 4/*next*/ + 4/*prev*/ + 32 /*property blocks*/; // = 41
    private final StoreChannel fileChannel;
    private final long maxId;

    public Legacy19PropertyStoreReader( FileSystemAbstraction fs, File file ) throws IOException
    {
        fileChannel = fs.open( file, "r" );
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_SIZE;
    }

    public Iterator<PropertyRecord> readPropertyStore() throws IOException
    {
        return new PrefetchingIterator<PropertyRecord>()
        {
            private long id = -1;
            ByteBuffer buffer = allocateDirect( RECORD_SIZE );

            @Override
            protected PropertyRecord fetchNextOrNull()
            {
                while ( ++id <= maxId )
                {
                    readIntoBuffer( fileChannel, buffer, RECORD_SIZE );
                    PropertyRecord record = readPropertyRecord( id, buffer );
                    if ( record.inUse() )
                    {
                        return record;
                    }
                }
                return null;
            }
        };
    }

    protected PropertyRecord readPropertyRecord( long id, ByteBuffer buffer )
    {
        PropertyRecord record = new PropertyRecord( id );

        /*
         * [pppp,nnnn] previous, next high bits
         */
        byte modifiers = buffer.get();
        long prevMod = ((modifiers & 0xF0L) << 28);
        long nextMod = ((modifiers & 0x0FL) << 32);
        long prevProp = getUnsignedInt( buffer );
        long nextProp = getUnsignedInt( buffer );
        record.setPrevProp( longFromIntAndMod( prevProp, prevMod ) );
        record.setNextProp( longFromIntAndMod( nextProp, nextMod ) );

        while ( buffer.hasRemaining() )
        {
            PropertyBlock newBlock = getPropertyBlock( buffer );
            if ( newBlock != null )
            {
                record.addPropertyBlock( newBlock );
                record.setInUse( true );
            }
            else
            {
                // We assume that storage is defragged
                break;
            }
        }
        return record;
    }

    private PropertyBlock getPropertyBlock( ByteBuffer buffer )
    {
        long header = buffer.getLong();
        PropertyType type = PropertyType.getPropertyType( header, true );
        if ( type == null )
        {
            return null;
        }
        PropertyBlock toReturn = new PropertyBlock();
        // toReturn.setInUse( true );
        int numBlocks = type.calculateNumberOfBlocksUsed( header );
        long[] blockData = new long[numBlocks];
        blockData[0] = header; // we already have that
        for ( int i = 1; i < numBlocks; i++ )
        {
            blockData[i] = buffer.getLong();
        }
        toReturn.setValueBlocks( blockData );
        return toReturn;
    }

    @Override
    public void close() throws IOException
    {
        fileChannel.close();
    }
}
