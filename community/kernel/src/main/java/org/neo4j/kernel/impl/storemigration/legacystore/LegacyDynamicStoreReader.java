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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindowPool;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.util.StringLogger;

public class LegacyDynamicStoreReader
{
    public static final String FROM_VERSION_ARRAY = "ArrayPropertyStore v0.9.9";
    public static final String FROM_VERSION_STRING = "StringPropertyStore v0.9.9";

    private final PersistenceWindowPool windowPool;
    private final int blockSize;

    // in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int)
    protected static final int BLOCK_HEADER_SIZE = 1 + 4 + 4 + 4;
    private final FileChannel fileChannel;

    public LegacyDynamicStoreReader( FileSystemAbstraction fs, File fileName, String fromVersionArray,
            StringLogger log ) throws IOException
    {
        fileChannel = fs.open( fileName, "r" );
        long fileSize = fileChannel.size();
        String expectedVersion = fromVersionArray;
        byte version[] = new byte[UTF8.encode( expectedVersion ).length];
        ByteBuffer buffer = ByteBuffer.wrap( version );
        fileChannel.position( fileSize - version.length );
        fileChannel.read( buffer );
        buffer = ByteBuffer.allocate( 4 );
        fileChannel.position( 0 );
        fileChannel.read( buffer );
        buffer.flip();
        blockSize = buffer.getInt();

        windowPool = new PersistenceWindowPool( fileName,
                blockSize, fileChannel, 0,
                true, true, log );
    }

    public List<LegacyDynamicRecord> getPropertyChain( long startBlockId )
    {
        List<LegacyDynamicRecord> recordList = new LinkedList<LegacyDynamicRecord>();
        long blockId = startBlockId;
        while ( blockId != Record.NO_NEXT_BLOCK.intValue() )
        {
            PersistenceWindow window = windowPool.acquire( blockId,
                    OperationType.READ );
            try
            {
                LegacyDynamicRecord record = getRecord( blockId, window );
                recordList.add( record );
                blockId = record.getNextBlock();
            }
            finally
            {
                windowPool.release( window );
            }
        }
        return recordList;
    }

    private LegacyDynamicRecord getRecord( long blockId, PersistenceWindow window )
    {
        LegacyDynamicRecord record = new LegacyDynamicRecord( blockId );
        Buffer buffer = window.getOffsettedBuffer( blockId );

        // [    ,   x] in use
        // [xxxx,    ] high bits for prev block
        long inUseByte = buffer.get();
        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( !inUse )
        {
            throw new InvalidRecordException( "Not in use, blockId[" + blockId + "]" );
        }
        long prevBlock = buffer.getUnsignedInt();
        long prevModifier = (inUseByte & 0xF0L) << 28;

        int dataSize = blockSize - BLOCK_HEADER_SIZE;

        // [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] number of bytes
        // [    ,xxxx][    ,    ][    ,    ][    ,    ] higher bits for next block
        long nrOfBytesInt = buffer.getInt();

        int nrOfBytes = (int) (nrOfBytesInt & 0xFFFFFF);

        long nextBlock = buffer.getUnsignedInt();
        long nextModifier = (nrOfBytesInt & 0xF000000L) << 8;

        long longNextBlock = LegacyStore.longFromIntAndMod( nextBlock, nextModifier );
        if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
                && nrOfBytes < dataSize || nrOfBytes > dataSize )
        {
            throw new InvalidRecordException( "Next block set[" + nextBlock
                    + "] current block illegal size[" + nrOfBytes + "/" + dataSize
                    + "]" );
        }
        record.setInUse( true );
        record.setLength( nrOfBytes );
        record.setPrevBlock( LegacyStore.longFromIntAndMod( prevBlock, prevModifier ) );
        record.setNextBlock( longNextBlock );
        byte byteArrayElement[] = new byte[nrOfBytes];
        buffer.get( byteArrayElement );
        record.setData( byteArrayElement );
        return record;
    }

    private static enum ArrayType
    {
        ILLEGAL( 0 ),
        INT( 1 ),
        STRING( 2 ),
        BOOL( 3 ),
        DOUBLE( 4 ),
        FLOAT( 5 ),
        LONG( 6 ),
        BYTE( 7 ),
        CHAR( 8 ),
        SHORT( 10 );

        private int type;

        ArrayType( int type )
        {
            this.type = type;
        }

        public byte byteValue()
        {
            return (byte) type;
        }
    }

    public static Object getRightArray( byte[] bArray )
    {
        ByteBuffer buf = ByteBuffer.wrap( bArray );
        byte type = buf.get();
        if ( type == ArrayType.INT.byteValue() )
        {
            int size = (bArray.length - 1) / 4;
            assert (bArray.length - 1) % 4 == 0;
            int[] array = new int[size];
            for ( int i = 0; i < size; i++ )
            {
                array[i] = buf.getInt();
            }
            return array;
        }
        if ( type == ArrayType.STRING.byteValue() )
        {
            String[] array = new String[buf.getInt()];
            for ( int i = 0; i < array.length; i++ )
            {
                int charLength = buf.getInt() / 2;
                char charBuffer[] = new char[charLength];
                for ( int j = 0; j < charLength; j++ )
                {
                    charBuffer[j] = buf.getChar();
                }
                array[i] = new String( charBuffer );
            }
            return array;
        }
        if ( type == ArrayType.BOOL.byteValue() )
        {
            boolean[] array = new boolean[buf.getInt()];
            int byteItr = 1;
            byte currentValue = buf.get();
            for ( int i = 0; i < array.length; i++ )
            {
                array[i] = (currentValue & byteItr) > 0 ? true : false;
                byteItr *= 2;
                if ( byteItr == 256 )
                {
                    byteItr = 0;
                    currentValue = buf.get();
                }
            }
            return array;
        }
        if ( type == ArrayType.DOUBLE.byteValue() )
        {
            int size = (bArray.length - 1) / 8;
            assert (bArray.length - 1) % 8 == 0;
            double[] array = new double[size];
            for ( int i = 0; i < size; i++ )
            {
                array[i] = buf.getDouble();
            }
            return array;
        }
        if ( type == ArrayType.FLOAT.byteValue() )
        {
            int size = (bArray.length - 1) / 4;
            assert (bArray.length - 1) % 4 == 0;
            float[] array = new float[size];
            for ( int i = 0; i < size; i++ )
            {
                array[i] = buf.getFloat();
            }
            return array;
        }
        if ( type == ArrayType.LONG.byteValue() )
        {
            int size = (bArray.length - 1) / 8;
            assert (bArray.length - 1) % 8 == 0;
            long[] array = new long[size];
            for ( int i = 0; i < size; i++ )
            {
                array[i] = buf.getLong();
            }
            return array;
        }
        if ( type == ArrayType.BYTE.byteValue() )
        {
            int size = (bArray.length - 1);
            byte[] array = new byte[size];
            buf.get( array );
            return array;
        }
        if ( type == ArrayType.CHAR.byteValue() )
        {
            int size = (bArray.length - 1) / 2;
            assert (bArray.length - 1) % 2 == 0;
            char[] array = new char[size];
            for ( int i = 0; i < size; i++ )
            {
                array[i] = buf.getChar();
            }
            return array;
        }
        if ( type == ArrayType.SHORT.byteValue() )
        {
            int size = (bArray.length - 1) / 2;
            assert (bArray.length - 1) % 2 == 0;
            short[] array = new short[size];
            for ( short i = 0; i < size; i++ )
            {
                array[i] = buf.getShort();
            }
            return array;
        }
        throw new InvalidRecordException( "Unknown array type[" + type + "]" );
    }

    public void close() throws IOException
    {
        fileChannel.close();
    }
}
