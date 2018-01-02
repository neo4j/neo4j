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

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store.getUnsignedInt;
import static org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store.longFromIntAndMod;
import static org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store.readIntoBuffer;

public class LegacyDynamicStringStoreReader
{
    private final int blockSize;
    private final StoreChannel fileChannel;
    private final ByteBuffer blockBuffer;
    private ByteBuffer chainBuffer;

    public LegacyDynamicStringStoreReader( FileSystemAbstraction fs, File fileName, String fromVersion )
            throws IOException
    {
        // Read version and block size (stored in the first record in the store)
        fileChannel = fs.open( fileName, "r" );
        long fileSize = fileChannel.size();
        byte version[] = new byte[UTF8.encode( fromVersion ).length];
        ByteBuffer buffer = ByteBuffer.wrap( version );
        fileChannel.position( fileSize - version.length );
        fileChannel.read( buffer );
        buffer = ByteBuffer.allocate( 4 );
        fileChannel.position( 0 );
        fileChannel.read( buffer );
        buffer.flip();
        blockSize = buffer.getInt();
        
        blockBuffer = ByteBuffer.allocate( blockSize );
        chainBuffer = ByteBuffer.wrap( new byte[blockSize*3] ); // just a default, will grow on demand
    }

    public String readDynamicString( long startRecordId ) throws IOException
    {
        long blockId = startRecordId;
        chainBuffer.clear();
        while ( blockId != Record.NO_NEXT_BLOCK.intValue() )
        {
            fileChannel.position( blockId*blockSize );
            readIntoBuffer( fileChannel, blockBuffer, blockSize );
            
            ensureChainBufferBigEnough();
            blockId = readRecord( blockId, blockBuffer );
        }
        return UTF8.decode( chainBuffer.array(), 0, chainBuffer.position() );
    }

    private void ensureChainBufferBigEnough()
    {
        if ( chainBuffer.remaining() < blockSize )
        {
            byte[] extendedBuffer = new byte[chainBuffer.capacity()*2];
            System.arraycopy( chainBuffer.array(), 0, extendedBuffer, 0, chainBuffer.capacity() );
            chainBuffer = ByteBuffer.wrap( extendedBuffer );
        }
    }

    private long readRecord( long blockId, ByteBuffer recordData )
    {
        /*
         * First 4b
         * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
         * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
         * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
         * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
         *
         */
        long firstInteger = getUnsignedInt( recordData );
        long maskedInteger = firstInteger & ~0x80000000;
        int highNibbleInMaskedInteger = (int) ( ( maskedInteger ) >> 28 );
        boolean inUse = highNibbleInMaskedInteger == Record.IN_USE.intValue();
        if ( !inUse )
        {
            throw new InvalidRecordException( "DynamicRecord not in use, blockId[" + blockId + "]" );
        }

        int nrOfBytes = (int) ( firstInteger & 0xFFFFFF );
        long nextBlock = getUnsignedInt( recordData );
        long nextModifier = ( firstInteger & 0xF000000L ) << 8;
        long longNextBlock = longFromIntAndMod( nextBlock, nextModifier );
        
        // Read the data into the chainBuffer
        recordData.limit( recordData.position()+nrOfBytes );
        chainBuffer.put( recordData );
        return longNextBlock;
    }

    public void close() throws IOException
    {
        fileChannel.close();
    }
}
