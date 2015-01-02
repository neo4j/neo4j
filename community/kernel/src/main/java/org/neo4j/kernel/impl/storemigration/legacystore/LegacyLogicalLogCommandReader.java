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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.xa.Command;

import static org.neo4j.kernel.impl.nioneo.xa.Command.NeoStoreCommand;
import static org.neo4j.kernel.impl.nioneo.xa.Command.NodeCommand;
import static org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import static org.neo4j.kernel.impl.nioneo.xa.Command.PropertyKeyTokenCommand;
import static org.neo4j.kernel.impl.nioneo.xa.Command.RelationshipCommand;
import static org.neo4j.kernel.impl.nioneo.xa.Command.RelationshipTypeTokenCommand;

/**
 * Reads log files from legacy (1.9) stores, and produces current (2.0) command objects from them.
 */
public class LegacyLogicalLogCommandReader implements LegacyLogCommandReader
{
    static PropertyBlock readPropertyBlock( ReadableByteChannel byteChannel,
                                            ByteBuffer buffer ) throws IOException
    {
        PropertyBlock toReturn = new PropertyBlock();
        buffer.clear();
        buffer.limit( 1 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte blockSize = buffer.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize
                + " is not a valid block size value";
        // Read in blocks
        buffer.clear();
        /*
         * We add 4 to avoid another limit()/read() for the DynamicRecord size
         * field later on
         */
        buffer.limit( blockSize + 4 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long[] blocks = readLongs( buffer, blockSize / 8 );
        assert blocks.length == blockSize / 8 : blocks.length
                + " longs were read in while i asked for what corresponds to "
                + blockSize;
        assert PropertyType.getPropertyType( blocks[0], false ).calculateNumberOfBlocksUsed(
                blocks[0] ) == blocks.length : blocks.length
                + " is not a valid number of blocks for type "
                + PropertyType.getPropertyType(
                blocks[0], false );
        /*
         *  Ok, now we may be ready to return, if there are no DynamicRecords. So
         *  we start building the Object
         */
        toReturn.setValueBlocks( blocks );

        /*
         * Read in existence of DynamicRecords. Remember, this has already been
         * read in the buffer with the blocks, above.
         */
        int noOfDynRecs = buffer.getInt();
        assert noOfDynRecs >= 0 : noOfDynRecs
                + " is not a valid value for the number of dynamic records in a property block";
        if ( noOfDynRecs != 0 )
        {
            for ( int i = 0; i < noOfDynRecs; i++ )
            {
                DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
                if ( dr == null )
                {
                    return null;
                }
                dr.setCreated(); // writePropertyBlock always writes only newly
                // created chains
                toReturn.addValueRecord( dr );
            }
            assert toReturn.getValueRecords().size() == noOfDynRecs : "read in "
                    + toReturn.getValueRecords().size()
                    + " instead of the proper "
                    + noOfDynRecs;
        }
        return toReturn;
    }

    static DynamicRecord readDynamicRecord( ReadableByteChannel byteChannel,
                                            ByteBuffer buffer ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        buffer.clear();
        buffer.limit( 13 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long id = buffer.getLong();
        assert id >= 0 && id <= ( 1l << 36 ) - 1 : id
                + " is not a valid dynamic record id";
        int type = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( inUseFlag == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }

        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            buffer.clear();
            buffer.limit( 12 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int nrOfBytes = buffer.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ( ( 1 << 24 ) - 1 ) : nrOfBytes
                    + " is not valid for a number of bytes field of a dynamic record";
            long nextBlock = buffer.getLong();
            assert ( nextBlock >= 0 && nextBlock <= ( 1l << 36 - 1 ) )
                    || ( nextBlock == Record.NO_NEXT_BLOCK.intValue() ) : nextBlock
                    + " is not valid for a next record field of a dynamic record";
            record.setNextBlock( nextBlock );
            buffer.clear();
            buffer.limit( nrOfBytes );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            byte data[] = new byte[nrOfBytes];
            buffer.get( data );
            record.setData( data );
        }
        return record;
    }

    private static long[] readLongs( ByteBuffer buffer, int count )
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = buffer.getLong();
        }
        return result;
    }

    // means the first byte of the command record was only written but second
    // (saying what type) did not get written but the file still got expanded
    private static final byte NONE = (byte) 0;
    private static final byte NODE_COMMAND = (byte) 1;
    private static final byte PROP_COMMAND = (byte) 2;
    private static final byte REL_COMMAND = (byte) 3;
    private static final byte REL_TYPE_COMMAND = (byte) 4;
    private static final byte PROP_INDEX_COMMAND = (byte) 5;
    private static final byte NEOSTORE_COMMAND = (byte) 6;

    public static Command readNodeCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        buffer.clear();
        buffer.limit( 9 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long id = buffer.getLong();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( inUseFlag == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        NodeRecord record;
        if ( inUse )
        {
            buffer.clear();
            buffer.limit( 16 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            record = new NodeRecord( id, buffer.getLong(), buffer.getLong() );
        }
        else record = new NodeRecord( id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() );
        record.setInUse( inUse );
        return new NodeCommand( null, record, record );
    }

    public static Command readRelationshipCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        buffer.clear();
        buffer.limit( 9 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long id = buffer.getLong();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
                .byteValue() )
        {
            inUse = true;
        }
        else if ( (inUseFlag & Record.IN_USE.byteValue()) != Record.NOT_IN_USE
                .byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        RelationshipRecord record;
        if ( inUse )
        {
            buffer.clear();
            buffer.limit( 60 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            record = new RelationshipRecord( id, buffer.getLong(), buffer
                    .getLong(), buffer.getInt() );
            record.setInUse( true );
            record.setFirstPrevRel( buffer.getLong() );
            record.setFirstNextRel( buffer.getLong() );
            record.setSecondPrevRel( buffer.getLong() );
            record.setSecondNextRel( buffer.getLong() );
            record.setNextProp( buffer.getLong() );
        }
        else
        {
            record = new RelationshipRecord( id, -1, -1, -1 );
            record.setInUse( false );
        }
        return new RelationshipCommand( null, record );
    }

    public static Command readNeoStoreCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        buffer.clear();
        buffer.limit( 8 );
        if ( byteChannel.read( buffer ) != buffer.limit() ) return null;
        buffer.flip();
        long nextProp = buffer.getLong();
        NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( nextProp );
        return new NeoStoreCommand( null, record );
    }

    public static Command readPropertyIndexCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
    {
        // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
        buffer.clear();
        buffer.limit( 17 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
                .byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( buffer.getInt() );
        record.setNameId( buffer.getInt() );
        int nrKeyRecords = buffer.getInt();
        for ( int i = 0; i < nrKeyRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
            if ( dr == null )
            {
                return null;
            }
            record.addNameRecord( dr );
        }
        return new PropertyKeyTokenCommand( null, record );
    }

    public static Command readPropertyCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        // id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
        // prev_prop_id(long)+next_prop_id(long)
        buffer.clear();
        buffer.limit( 8 + 1 + 8 + 8 + 8 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();

        long id = buffer.getLong(); // 8
        PropertyRecord record = new PropertyRecord( id );
        byte inUseFlag = buffer.get(); // 1
        long nextProp = buffer.getLong(); // 8
        long prevProp = buffer.getLong(); // 8
        record.setNextProp( nextProp );
        record.setPrevProp( prevProp );
        boolean inUse = false;
        if ( ( inUseFlag & Record.IN_USE.byteValue() ) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        boolean nodeProperty = true;
        if ( ( inUseFlag & Record.REL_PROPERTY.byteValue() ) == Record.REL_PROPERTY.byteValue() )
        {
            nodeProperty = false;
        }
        long primitiveId = buffer.getLong(); // 8
        if ( primitiveId != -1 && nodeProperty )
        {
            record.setNodeId( primitiveId );
        }
        else if ( primitiveId != -1 )
        {
            record.setRelId( primitiveId );
        }
        buffer.clear();
        buffer.limit( 1 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int nrPropBlocks = buffer.get(); // 1
        assert nrPropBlocks >= 0;
        if ( nrPropBlocks > 0 )
        {
            record.setInUse( true );
        }
        while ( nrPropBlocks-- > 0 )
        {
            PropertyBlock block = readPropertyBlock( byteChannel, buffer );
            if ( block == null )
            {
                return null;
            }
            record.addPropertyBlock( block );
        }
        // Time to read in the deleted dynamic records
        buffer.clear();
        buffer.limit( 4 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int deletedRecords = buffer.getInt(); // 4
        assert deletedRecords >= 0;
        while ( deletedRecords-- > 0 )
        {
            DynamicRecord read = readDynamicRecord( byteChannel, buffer );
            if ( read == null )
            {
                return null;
            }
            record.addDeletedRecord( read );
        }

        if ( ( inUse && !record.inUse() ) || ( !inUse && record.inUse() ) )
        {
            throw new IllegalStateException( "Weird, inUse was read in as "
                    + inUse
                    + " but the record is "
                    + record );
        }
        return new PropertyCommand( null, record, record );
    }

    public static Command readRelationshipTypeCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        buffer.clear();
        buffer.limit( 13 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) ==
                Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        record.setInUse( inUse );
        record.setNameId( buffer.getInt() );
        int nrTypeRecords = buffer.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
            if ( dr == null )
            {
                return null;
            }
            record.addNameRecord( dr );
        }
        return new RelationshipTypeTokenCommand( null, record );
    }

    @Override
    public Command readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
    {
        buffer.clear();
        buffer.limit( 1 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte commandType = buffer.get();
        switch ( commandType )
        {
            case NODE_COMMAND:
                return readNodeCommand( byteChannel, buffer );
            case PROP_COMMAND:
                return readPropertyCommand( byteChannel, buffer );
            case PROP_INDEX_COMMAND:
                return readPropertyIndexCommand( byteChannel, buffer );
            case REL_COMMAND:
                return readRelationshipCommand( byteChannel, buffer );
            case REL_TYPE_COMMAND:
                return readRelationshipTypeCommand( byteChannel, buffer );
            case NEOSTORE_COMMAND:
                return readNeoStoreCommand( byteChannel, buffer );
            case NONE: return null;
            default:
                throw new IOException( "Unknown command type[" + commandType + "]" );
        }
    }
}
