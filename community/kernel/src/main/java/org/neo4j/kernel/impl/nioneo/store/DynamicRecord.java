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
package org.neo4j.kernel.impl.nioneo.store;

import org.neo4j.io.pagecache.PageIO;
import org.neo4j.io.pagecache.impl.common.Page;

public class DynamicRecord extends Abstract64BitRecord implements PageIO
{
    private static final int MAX_BYTES_IN_TO_STRING = 8, MAX_CHARS_IN_TO_STRING = 16;
    public static final long IO_WRITE = 1;
    public static final long IO_READ_HEADER = 1 << 1;
    public static final long IO_READ_DATA = 1 << 2;
    public static final long IO_READ_FORCE = 1 << 3;

    private byte[] data = null;
    private int length;
    private long nextBlock = Record.NO_NEXT_BLOCK.intValue();
    private int type;
    private boolean startRecord = true;

    public static DynamicRecord dynamicRecord( long id, boolean inUse )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse );
        return record;
    }

    public static DynamicRecord dynamicRecord( long id, boolean inUse, boolean isStartRecord, long nextBlock, int type,
                                               byte [] data )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse );
        record.setStartRecord( isStartRecord );
        record.setNextBlock( nextBlock );
        record.setType( type );
        record.setData( data );
        return record;
    }

    @Override
    public void apply( long pageId, Page page, long io_context, long io_flags )
    {
        int offset = (int) io_context;
        if ( (io_flags & IO_WRITE) != 0 )
        {
            writeRecord( page, offset );
            return;
        }
        boolean hasDataToRead = true;
        if ( (io_flags & IO_READ_HEADER) != 0 )
        {
            hasDataToRead = readRecordHeader( page, offset, io_flags );
        }
        if ( (io_flags & IO_READ_DATA) != 0 && hasDataToRead )
        {
            readRecordData( page, offset );
        }
    }

    private boolean readRecordHeader( Page page, int offset, long io_flags )
    {
        /*
         * First 4b
         * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
         * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
         * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
         * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
         *
         */
        long firstInteger = page.getUnsignedInt( offset );
        boolean force = (io_flags & IO_READ_FORCE) != 0;
        boolean isStartRecord = (firstInteger & 0x80000000) == 0;
        long maskedInteger = firstInteger & ~0x80000000;
        int highNibbleInMaskedInteger = (int) ( ( maskedInteger ) >> 28 );
        boolean inUse = highNibbleInMaskedInteger == Record.IN_USE.intValue();
        if ( !inUse && !force )
        {
            throw new InvalidRecordException( "DynamicRecord Not in use, blockId[" + getId() + "]" );
        }
        int dataSize = decodeBlockSizeFlags( io_flags ) - AbstractDynamicStore.BLOCK_HEADER_SIZE;

        int nrOfBytes = (int) ( firstInteger & 0xFFFFFF );

        /*
         * Pointer to next block 4b (low bits of the pointer)
         */
        long nextBlock = page.getUnsignedInt( offset + 4 );
        long nextModifier = ( firstInteger & 0xF000000L ) << 8;

        long longNextBlock = CommonAbstractStore.longFromIntAndMod( nextBlock, nextModifier );
        boolean hasDataToRead = true;
        if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
                && nrOfBytes < dataSize || nrOfBytes > dataSize )
        {
            hasDataToRead = false;
            if ( !force )
            {
                throw new InvalidRecordException( "Next block set[" + nextBlock
                        + "] current block illegal size[" + nrOfBytes + "/" + dataSize + "]" );
            }
        }
        setInUse( inUse );
        setStartRecord( isStartRecord );
        setLength( nrOfBytes );
        setNextBlock( longNextBlock );
        return hasDataToRead;
    }

    public static long encodeBlockSizeFlags( int blockSize, long io_flags )
    {
        return io_flags + ((long) blockSize << 32);
    }

    public static int decodeBlockSizeFlags( long io_flags )
    {
        return (int) (io_flags >> 32 & 0xFFFFFFFFL);
    }

    private void readRecordData( Page page, int offset )
    {
        int len = getLength();
        if ( data == null || data.length != len )
        {
            data = new byte[len];
        }
        page.getBytes( data, offset + AbstractDynamicStore.BLOCK_HEADER_SIZE );
    }

    private void writeRecord( Page page, int offset )
    {
        if ( inUse() )
        {
            long nextBlock = getNextBlock();
            int highByteInFirstInteger = nextBlock == Record.NO_NEXT_BLOCK.intValue() ? 0
                    : (int) ( ( nextBlock & 0xF00000000L ) >> 8 );
            highByteInFirstInteger |= ( Record.IN_USE.byteValue() << 28 );
            highByteInFirstInteger |= (isStartRecord() ? 0 : 1) << 31;

            /*
             * First 4b
             * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
             * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
             * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
             * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
             *
             */
            int firstInteger = getLength();
            assert firstInteger < ( 1 << 24 ) - 1;

            firstInteger |= highByteInFirstInteger;

            page.putInt( firstInteger, offset );
            page.putInt( (int) nextBlock, offset + 4 );
            if ( !isLight() )
            {
                page.putBytes( getData(), offset + 8 );
            }
        }
        else
        {
            page.putByte( Record.NOT_IN_USE.byteValue(), offset );
        }
    }

    public DynamicRecord( long id )
    {
        super( id );
    }
    
    public void setStartRecord( boolean startRecord )
    {
        this.startRecord = startRecord;
    }
    
    public boolean isStartRecord()
    {
        return startRecord;
    }

    public int getType()
    {
        return type;
    }

    public void setType( int type )
    {
        this.type = type;
    }

    public boolean isLight()
    {
        return data == null;
    }

    public void setLength( int length )
    {
        this.length = length;
    }

    @Override
    public void setInUse( boolean inUse )
    {
        super.setInUse( inUse );
        if ( !inUse )
        {
            data = null;
        }
    }

    public void setInUse( boolean inUse, int type )
    {
        this.type = type;
        this.setInUse( inUse );
    }

    public void setData( byte[] data )
    {
        this.length = data.length;
        this.data = data;
    }

    public int getLength()
    {
        return length;
    }

    public byte[] getData()
    {
        return data;
    }

    public long getNextBlock()
    {
        return nextBlock;
    }

    public void setNextBlock( long nextBlock )
    {
        this.nextBlock = nextBlock;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        buf.append( "DynamicRecord[" )
                .append( getId() )
                .append( ",used=" ).append(inUse() ).append( "," )
                .append( "light=" ).append( isLight() )
                .append("(" ).append( length ).append( "),type=" );
        PropertyType type = PropertyType.getPropertyType( this.type << 24, true );
        if ( type == null ) buf.append( this.type ); else buf.append( type.name() );
        buf.append( ",data=" );
        if ( data != null )
        {
            if ( type == PropertyType.STRING && data.length <= MAX_CHARS_IN_TO_STRING )
            {
                buf.append( '"' );
                buf.append( PropertyStore.decodeString( data ) );
                buf.append( "\"," );
            }
            else
            {
                buf.append( "byte[" );
                if ( data.length <= MAX_BYTES_IN_TO_STRING )
                {
                    for ( int i = 0; i < data.length; i++ )
                    {
                        if (i != 0) buf.append( ',' );
                        buf.append( data[i] );
                    }
                }
                else
                {
                    buf.append( "size=" ).append( data.length );
                }
                buf.append( "]," );
            }
        }
        else
        {
            buf.append( "null," );
        }
        buf.append( "start=" ).append( startRecord );
        buf.append( ",next=" ).append( nextBlock ).append( "]" );
        return buf.toString();
    }
    
    @Override
    public DynamicRecord clone()
    {
        DynamicRecord result = new DynamicRecord( getLongId() );
        if ( data != null )
            result.data = data.clone();
        result.setInUse( inUse() );
        result.length = length;
        result.nextBlock = nextBlock;
        result.type = type;
        result.startRecord = startRecord;
        return result;
    }
    
    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof DynamicRecord ) )
            return false;
        return ((DynamicRecord) obj).getId() == getId();
    }
    
    @Override
    public int hashCode()
    {
        long id = getId();
        return (int) (( id >>> 32 ) ^ id );
    }
}
