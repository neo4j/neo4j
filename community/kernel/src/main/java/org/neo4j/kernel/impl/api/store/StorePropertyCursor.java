/*
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
package org.neo4j.kernel.impl.api.store;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import org.neo4j.cursor.GenericCursor;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Cursor for all properties on a node or relationship.
 */
public class StorePropertyCursor implements PropertyCursor
{
    private static final long KEY_BITMASK = 0xFFFFFFL;
    private static final int BITS_BYTE_SIZE = 32;
    private static final int INTERNAL_BYTE_ARRAY_SIZE = 4096;

    private final PropertyStore propertyStore;
    private final DynamicStringStore stringStore;
    private final DynamicArrayStore arrayStore;
    private final InstanceCache<StorePropertyCursor> instanceCache;

    private long nextPropertyRecordId;
    private PageCursor cursor;

    private int offsetAtBeginning;
    private int remainingBlocksToRead;
    private long header;
    private PropertyType type;
    private int keyId;

    private byte[] bytes = new byte[INTERNAL_BYTE_ARRAY_SIZE];
    private ByteBuffer buffer = ByteBuffer.wrap( bytes ).order( ByteOrder.LITTLE_ENDIAN );
    private Bits bits = Bits.bits( BITS_BYTE_SIZE );

    private AbstractDynamicStore.DynamicRecordCursor stringRecordCursor;
    private AbstractDynamicStore.DynamicRecordCursor arrayRecordCursor;
    private long currentRecordId;
    private long originalHeader;

    public StorePropertyCursor( PropertyStore propertyStore,
            InstanceCache<StorePropertyCursor> instanceCache )
    {
        this.propertyStore = propertyStore;
        this.stringStore = propertyStore.getStringStore();
        this.arrayStore = propertyStore.getArrayStore();
        this.instanceCache = instanceCache;
    }

    public StorePropertyCursor init( long firstPropertyId )
    {
        nextPropertyRecordId = firstPropertyId;
        cursor = null;
        remainingBlocksToRead = 0;
        return this;
    }

    @Override
    public boolean next()
    {
        do
        {
            if ( cursor == null )
            {
                if ( nextPropertyRecordId == Record.NO_NEXT_PROPERTY.intValue() )
                {
                    return false;
                }

                nextRecord();
            }

        } while ( !nextBlock() );

        return true;
    }

    @Override
    public boolean seek( int keyId )
    {
        while ( next() )
        {
            if ( propertyKeyId() == keyId )
            {
                return true;
            }
        }

        return false;
    }

    public int propertyKeyId()
    {
        return keyId;
    }

    @Override
    public Object value()
    {
        if ( type == null )
        {
            throw new IllegalStateException();
        }

        switch ( type )
        {
            case BOOL:
            {
                return header == 1;
            }

            case BYTE:
            {
                return (byte) header;
            }

            case SHORT:
            {
                return (short) header;
            }

            case CHAR:
            {
                return (char) header;
            }

            case INT:
            {
                return (int) header;
            }

            case LONG:
            {
                if ( (header & 0x1L) > 0 )
                {
                    return header >>> 1;
                }
                else
                {
                    remainingBlocksToRead--;
                    return cursor.getLong();
                }
            }

            case FLOAT:
            {
                return Float.intBitsToFloat( (int) header );
            }

            case DOUBLE:
            {
                remainingBlocksToRead--;
                return Double.longBitsToDouble( cursor.getLong() );
            }

            case STRING:
            {
                try
                {
                    readPropertyData();
                    return UTF8.decode( bytes, 0, buffer.limit() );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }

            case ARRAY:
            {
                try
                {
                    readPropertyData();
                    return getRightArray();
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }

            case SHORT_STRING:
            {
                try
                {
                    readPropertyData();
                    bits.clear( true );
                    bits.put( bytes, 0, buffer.limit() );
                    return LongerShortString.decode( bits );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }

            case SHORT_ARRAY:
            {
                try
                {
                    readPropertyData();
                    bits.clear( true );
                    bits.put( bytes, 0, buffer.limit() );
                    return ShortArray.decode( bits );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }

            default:
            {
                throw new IllegalStateException( "No such type:" + type );
            }
        }
    }

    @Override
    public boolean booleanValue()
    {
        if ( type == null )
        {
            throw new IllegalStateException();
        }

        if ( type == PropertyType.BOOL )
        {
            return header == 1;
        }
        else
        {
            throw new IllegalStateException( "Not a boolean type:" + type );
        }
    }

    @Override
    public long longValue()
    {
        if ( type == null )
        {
            throw new IllegalStateException();
        }

        switch ( type )
        {
            case BOOL:
            case BYTE:
            case SHORT:
            case CHAR:
            case INT:
            {
                return header;
            }

            case LONG:
            {
                if ( (header & 0x1L) > 0 )
                {
                    return header >>> 1;
                }
                else
                {
                    remainingBlocksToRead--;
                    return cursor.getLong();
                }
            }


            default:
            {
                throw new IllegalStateException( "Not an integral type:" + type );
            }
        }
    }

    @Override
    public double doubleValue()
    {
        if ( type == null )
        {
            throw new IllegalStateException();
        }

        switch ( type )
        {
            case FLOAT:
            {
                return Float.intBitsToFloat( (int) header );
            }

            case DOUBLE:
            {
                remainingBlocksToRead--;
                return Double.longBitsToDouble( cursor.getLong() );
            }

            default:
            {
                throw new IllegalStateException( "Not a real number type:" + type );
            }
        }
    }

    @Override
    public String stringValue()
    {
        return value().toString();
    }

    public void propertyData( WritableByteChannel channel )
    {
        try
        {
            readPropertyData();

            channel.write( buffer );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void readPropertyData() throws IOException
    {
        buffer.clear();

        switch ( type )
        {
            case BOOL:
            case BYTE:
            {
                buffer.put( (byte) header );
                break;
            }

            case SHORT:
            {
                buffer.putShort( (short) header );
                break;
            }

            case CHAR:
            {
                buffer.putChar( (char) header );
                break;
            }

            case INT:
            {
                buffer.putInt( (int) header );
                break;
            }

            case LONG:
            {
                if ( (header & 0x1L) > 0 )
                {
                    buffer.putLong( header >>> 1 );
                }
                else
                {
                    buffer.putLong( cursor.getLong() );
                    remainingBlocksToRead--;
                }
                break;
            }

            case FLOAT:
            {
                buffer.putInt( (int) header );
                break;
            }

            case DOUBLE:
            {
                buffer.putLong( cursor.getLong() );
                remainingBlocksToRead--;
                break;
            }

            case STRING:
            {
                int storeOffset = cursor.getOffset();
                cursor.close();

                if ( stringRecordCursor == null )
                {
                    stringRecordCursor = stringStore.newDynamicRecordCursor();
                }

                try ( GenericCursor<DynamicRecord> stringRecords = stringStore.getRecordsCursor( header, true,
                        stringRecordCursor ) )
                {
                    while ( stringRecords.next() )
                    {
                        DynamicRecord dynamicRecord = stringRecords.get();

                        buffer.put( dynamicRecord.getData(), 0, dynamicRecord.getData().length );
                    }
                }

                cursor = propertyStore.newReadCursor( currentRecordId );
                cursor.setOffset( storeOffset );
                break;
            }

            case ARRAY:
            {
                int storeOffset = cursor.getOffset();
                cursor.close();

                if ( arrayRecordCursor == null )
                {
                    arrayRecordCursor = arrayStore.newDynamicRecordCursor();
                }

                try ( GenericCursor<DynamicRecord> arrayRecords = arrayStore.getRecordsCursor( header, true,
                        arrayRecordCursor ) )
                {
                    while ( arrayRecords.next() )
                    {
                        DynamicRecord dynamicRecord = arrayRecords.get();

                        while ( true )
                        {
                            try
                            {
                                buffer.put( dynamicRecord.getData(), 0, dynamicRecord.getData().length );
                                break;
                            }
                            catch ( BufferOverflowException e )
                            {
                                buffer.flip();
                                bytes = new byte[bytes.length * 2];
                                ByteBuffer newBuffer = ByteBuffer.wrap( bytes ).order( ByteOrder.LITTLE_ENDIAN );
                                newBuffer.put( buffer );
                                buffer = newBuffer;
                            }
                        }
                    }
                }

                cursor = propertyStore.newReadCursor( currentRecordId );
                cursor.setOffset( storeOffset );
                break;
            }

            case SHORT_STRING:
            case SHORT_ARRAY:
            {
                buffer.putLong( originalHeader );
                while ( remainingBlocksToRead-- > 0 )
                {
                    buffer.putLong( cursor.getLong() );
                }
                break;
            }

            default:
            {
                throw new IllegalStateException();
            }
        }

        buffer.flip();
    }

    @Override
    public void close()
    {
        if ( cursor != null )
        {
            cursor.close();
            cursor = null;
        }

        instanceCache.accept( this );
    }


    private void nextRecord()
    {
        try
        {
            cursor = propertyStore.newReadCursor( nextPropertyRecordId );
            currentRecordId = nextPropertyRecordId;

            offsetAtBeginning = cursor.getOffset();

            byte modifiers = cursor.getByte();
            long nextMod = (modifiers & 0x0FL) << 32;
            cursor.getUnsignedInt(); // We don't care about previous pointer
            long nextProp = cursor.getUnsignedInt();
            nextPropertyRecordId = longFromIntAndMod( nextProp, nextMod );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private boolean nextBlock()
    {
        // Read remaining data from previous property (if it was not read)
        while ( remainingBlocksToRead-- > 0 )
        {
            cursor.getLong();
        }

        if ( cursor.getOffset() - offsetAtBeginning < PropertyStore.RECORD_SIZE )
        {
            header = cursor.getLong();

            type = getPropertyType( header );
            if ( type != null )
            {
                keyId = (int) (header & KEY_BITMASK);

                remainingBlocksToRead = type.calculateNumberOfBlocksUsed( header ) - 1;

                originalHeader = header;
                header = header >>> 28;

                return true;
            }
        }

        cursor.close();
        cursor = null;

        return false;
    }

    private long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    private PropertyType getPropertyType( long propBlock )
    {
        // [][][][][    ,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        int type = (int) ((propBlock & 0x000000000F000000L) >> 24);
        switch ( type )
        {
            case 1:
                return PropertyType.BOOL;
            case 2:
                return PropertyType.BYTE;
            case 3:
                return PropertyType.SHORT;
            case 4:
                return PropertyType.CHAR;
            case 5:
                return PropertyType.INT;
            case 6:
                return PropertyType.LONG;
            case 7:
                return PropertyType.FLOAT;
            case 8:
                return PropertyType.DOUBLE;
            case 9:
                return PropertyType.STRING;
            case 10:
                return PropertyType.ARRAY;
            case 11:
                return PropertyType.SHORT_STRING;
            case 12:
                return PropertyType.SHORT_ARRAY;
            default:
            {
                return null;
            }
        }
    }

    private Object getRightArray()
    {
        byte typeId = buffer.get();
        buffer.order( ByteOrder.BIG_ENDIAN );
        try
        {
            if ( typeId == PropertyType.STRING.intValue() )
            {
                int arrayLength = buffer.getInt();
                String[] result = new String[arrayLength];

                for ( int i = 0; i < arrayLength; i++ )
                {
                    int byteLength = buffer.getInt();
                    result[i] = UTF8.decode( bytes, buffer.position(), byteLength );
                    buffer.position( buffer.position() + byteLength );
                }
                return result;
            }
            else
            {
                ShortArray type = ShortArray.typeOf( typeId );
                int bitsUsedInLastByte = buffer.get();
                int requiredBits = buffer.get();
                if ( requiredBits == 0 )
                {
                    return type.createEmptyArray();
                }
                Object result;
                if ( type == ShortArray.BYTE && requiredBits == Byte.SIZE )
                {   // Optimization for byte arrays (probably large ones)
                    byte[] byteArray = new byte[buffer.limit() - buffer.position()];
                    buffer.get( byteArray );
                    result = byteArray;
                }
                else
                {   // Fallback to the generic approach, which is a slower
                    Bits bits = Bits.bitsFromBytes( bytes, buffer.position() );
                    int length = ((buffer.limit() - buffer.position()) * 8 - (8 - bitsUsedInLastByte)) / requiredBits;
                    result = type.createArray( length, bits, requiredBits );
                }
                return result;
            }
        }
        finally
        {
            buffer.order( ByteOrder.LITTLE_ENDIAN );
        }
    }
}
