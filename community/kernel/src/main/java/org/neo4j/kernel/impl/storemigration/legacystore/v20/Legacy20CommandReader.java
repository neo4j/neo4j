/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyLogIoUtil;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.nioneo.store.Record.IN_USE;
import static org.neo4j.kernel.impl.nioneo.store.Record.NOT_IN_USE;
import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readAndFlip;

// most of the code has been copied from  org.neo4j.kernel.impl.nioneo.xa.Command
public class Legacy20CommandReader implements LegacyLogIoUtil.CommandReader
{
    private interface DynamicRecordAdder<T>
    {
        void add( T target, DynamicRecord record );
    }

    private static final DynamicRecordAdder<PropertyBlock> PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyBlock>()
            {
                @Override
                public void add( PropertyBlock target, DynamicRecord record )
                {
                    record.setCreated();
                    target.addValueRecord( record );
                }
            };


    private static PropertyBlock readPropertyBlock( ReadableByteChannel byteChannel,
                                                    ByteBuffer buffer ) throws IOException
    {
        PropertyBlock toReturn = new PropertyBlock();
        if ( !readAndFlip( byteChannel, buffer, 1 ) )
        {
            return null;
        }
        byte blockSize = buffer.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize
                + " is not a valid block size value";
        // Read in blocks
        if ( !readAndFlip( byteChannel, buffer, blockSize ) )
        {
            return null;
        }
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
        if ( !readDynamicRecords( byteChannel, buffer, toReturn, PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER ) )
        {
            return null;
        }

        return toReturn;
    }

    private static <T> boolean readDynamicRecords( ReadableByteChannel byteChannel, ByteBuffer buffer,
                                                   T target, DynamicRecordAdder<T> adder ) throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 4 ) )
        {
            return false;
        }
        int numberOfRecords = buffer.getInt();
        assert numberOfRecords >= 0;
        while ( numberOfRecords-- > 0 )
        {
            DynamicRecord read = readDynamicRecord( byteChannel, buffer );
            if ( read == null )
            {
                return false;
            }
            adder.add( target, read );
        }
        return true;
    }

    private static DynamicRecord readDynamicRecord( ReadableByteChannel byteChannel,
                                                    ByteBuffer buffer ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( !readAndFlip( byteChannel, buffer, 13 ) )
        {
            return null;
        }
        long id = buffer.getLong();
        assert id >= 0 && id <= (1l << 36) - 1 : id
                + " is not a valid dynamic record id";
        int type = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = (inUseFlag & IN_USE.byteValue()) != 0;

        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            record.setStartRecord( (inUseFlag & Record.FIRST_IN_CHAIN.byteValue()) != 0 );
            if ( !readAndFlip( byteChannel, buffer, 12 ) )
            {
                return null;
            }
            int nrOfBytes = buffer.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ((1 << 24) - 1) : nrOfBytes
                    + " is not valid for a number of bytes field of a dynamic record";
            long nextBlock = buffer.getLong();
            assert (nextBlock >= 0 && nextBlock <= (1l << 36 - 1))
                    || (nextBlock == Record.NO_NEXT_BLOCK.intValue()) : nextBlock
                    + " is not valid for a next record field of a dynamic record";
            record.setNextBlock( nextBlock );
            if ( !readAndFlip( byteChannel, buffer, nrOfBytes ) )
            {
                return null;
            }
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
    private static final byte SCHEMA_RULE_COMMAND = (byte) 7;
    private static final byte LABEL_KEY_COMMAND = (byte) 8;

    @Override
    public XaCommand readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 1 ) )
        {
            return null;
        }
        byte commandType = buffer.get();
        switch ( commandType )
        {
            case NODE_COMMAND:
                return readNodeCommand( byteChannel, buffer );
            case PROP_COMMAND:
                return readPropertyCommand( byteChannel, buffer );
            case PROP_INDEX_COMMAND: // PropertyKeyTokenCommand
                return readPropertyKeyTokenCommand( byteChannel, buffer );
            case REL_COMMAND:
                return readRelationshipCommand( byteChannel, buffer );
            case REL_TYPE_COMMAND:
                return readRelationshipTypeTokenCommand( byteChannel, buffer );
            case LABEL_KEY_COMMAND:
                return readLabelTokenCommand( byteChannel, buffer );
            case NEOSTORE_COMMAND:
                return readNeoStoreCommand( byteChannel, buffer );
            case SCHEMA_RULE_COMMAND:
                return readSchemaRuleCommand( byteChannel, buffer );
            case NONE:
                return null;
            default:
                throw new IOException( "Unknown command type[" + commandType + "]" );
        }
    }

    private static Command readNodeCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 8 ) )
        {
            return null;
        }
        long id = buffer.getLong();

        NodeRecord before = readNodeRecord( id, byteChannel, buffer );
        if ( before == null )
        {
            return null;
        }

        NodeRecord after = readNodeRecord( id, byteChannel, buffer );
        if ( after == null )
        {
            return null;
        }

        if ( !before.inUse() && after.inUse() )
        {
            after.setCreated();
        }

        return new Command.NodeCommand().init( before, after );
    }

    private static NodeRecord readNodeRecord( long id, ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 1 ) )
        {
            return null;
        }
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( inUseFlag == IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        NodeRecord record;
        if ( inUse )
        {
            if ( !readAndFlip( byteChannel, buffer, 8 * 3 ) )
            {
                return null;
            }
            record = new NodeRecord( id, false, buffer.getLong(), buffer.getLong() );

            // labels
            long labelField = buffer.getLong();
            Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<>();
            readDynamicRecords( byteChannel, buffer, dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
            record.setLabelField( labelField, dynamicLabelRecords );
        }
        else
        {
            record = new NodeRecord( id, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
        }

        record.setInUse( inUse );
        return record;
    }


    private static Command readPropertyCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        // ID
        if ( !readAndFlip( byteChannel, buffer, 8 ) )
        {
            return null;
        }
        long id = buffer.getLong(); // 8

        // BEFORE
        PropertyRecord before = readPropertyRecord( id, byteChannel, buffer );
        if ( before == null )
        {
            return null;
        }

        // AFTER
        PropertyRecord after = readPropertyRecord( id, byteChannel, buffer );
        if ( after == null )
        {
            return null;
        }

        return new Command.PropertyCommand().init( before, after );
    }


    private static PropertyRecord readPropertyRecord( long id, ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        // in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
        // prev_prop_id(long)+next_prop_id(long)
        if ( !readAndFlip( byteChannel, buffer, 1 + 8 + 8 + 8 ) )
        {
            return null;
        }

        PropertyRecord record = new PropertyRecord( id );
        byte inUseFlag = buffer.get(); // 1
        long nextProp = buffer.getLong(); // 8
        long prevProp = buffer.getLong(); // 8
        record.setNextProp( nextProp );
        record.setPrevProp( prevProp );
        boolean inUse = false;
        if ( (inUseFlag & IN_USE.byteValue()) == IN_USE.byteValue() )
        {
            inUse = true;
        }
        boolean nodeProperty = true;
        if ( (inUseFlag & Record.REL_PROPERTY.byteValue()) == Record.REL_PROPERTY.byteValue() )
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
        if ( !readAndFlip( byteChannel, buffer, 1 ) )
        {
            return null;
        }
        int nrPropBlocks = buffer.get();
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

        if ( !readDynamicRecords( byteChannel, buffer, record, PROPERTY_DELETED_DYNAMIC_RECORD_ADDER ) )
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

        if ( (inUse && !record.inUse()) || (!inUse && record.inUse()) )
        {
            throw new IllegalStateException( "Weird, inUse was read in as "
                    + inUse
                    + " but the record is "
                    + record );
        }
        return record;
    }

    private static final DynamicRecordAdder<PropertyRecord> PROPERTY_DELETED_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyRecord>()
            {
                @Override
                public void add( PropertyRecord target, DynamicRecord record )
                {
                    assert !record.inUse() : record + " is kinda weird";
                    target.addDeletedRecord( record );
                }
            };

    private static Command readPropertyKeyTokenCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        // id+in_use(byte)+count(int)+key_blockId(int)
        if ( !readAndFlip( byteChannel, buffer, 13 ) )
        {
            return null;
        }
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & IN_USE.byteValue()) == IN_USE
                .byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( buffer.getInt() );
        record.setNameId( buffer.getInt() );
        if ( !readDynamicRecords( byteChannel, buffer, record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER ) )
        {
            return null;
        }
        return new Command.PropertyKeyTokenCommand().init( record );
    }

    private static final DynamicRecordAdder<PropertyKeyTokenRecord> PROPERTY_INDEX_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyKeyTokenRecord>()
            {
                @Override
                public void add( PropertyKeyTokenRecord target, DynamicRecord record )
                {
                    target.addNameRecord( record );
                }
            };


    private static Command readRelationshipCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 9 ) )
        {
            return null;
        }
        long id = buffer.getLong();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & IN_USE.byteValue()) == IN_USE
                .byteValue() )
        {
            inUse = true;
        }
        else if ( (inUseFlag & IN_USE.byteValue()) != NOT_IN_USE
                .byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        RelationshipRecord record;
        if ( inUse )
        {
            if ( !readAndFlip( byteChannel, buffer, 60 ) )
            {
                return null;
            }
            record = new RelationshipRecord( id, buffer.getLong(), buffer
                    .getLong(), buffer.getInt() );
            record.setInUse( inUse );
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
        return new Command.RelationshipCommand().init( record );
    }

    private static Command readRelationshipTypeTokenCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        if ( !readAndFlip( byteChannel, buffer, 13 ) )
        {
            return null;
        }
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & IN_USE.byteValue()) ==
                IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != NOT_IN_USE.byteValue() )
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
        return new Command.RelationshipTypeTokenCommand().init( record );
    }

    private static Command readLabelTokenCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        if ( !readAndFlip( byteChannel, buffer, 13 ) )
        {
            return null;
        }
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & IN_USE.byteValue()) ==
                IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        LabelTokenRecord record = new LabelTokenRecord( id );
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
        return new Command.LabelTokenCommand().init( record );
    }

    private static Command readNeoStoreCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 8 ) )
        {
            return null;
        }
        long nextProp = buffer.getLong();
        NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( nextProp );
        return new Command.NeoStoreCommand().init( record );
    }

    private static Command readSchemaRuleCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
    {
        Collection<DynamicRecord> recordsBefore = new ArrayList<>();
        readDynamicRecords( byteChannel, buffer, recordsBefore, COLLECTION_DYNAMIC_RECORD_ADDER );

        Collection<DynamicRecord> recordsAfter = new ArrayList<>();
        readDynamicRecords( byteChannel, buffer, recordsAfter, COLLECTION_DYNAMIC_RECORD_ADDER );

        if ( !readAndFlip( byteChannel, buffer, 1 ) )
        {
            throw new IllegalStateException( "Missing SchemaRule.isCreated flag in deserialization" );
        }

        byte isCreated = buffer.get();
        if ( 1 == isCreated )
        {
            for ( DynamicRecord record : recordsAfter )
            {
                record.setCreated();
            }
        }

        if ( !readAndFlip( byteChannel, buffer, 8 ) )
        {
            throw new IllegalStateException( "Missing SchemaRule.txId in deserialization" );
        }

        long txId = buffer.getLong();

        SchemaRule rule = first( recordsAfter ).inUse() ?
                readSchemaRule( recordsAfter ) :
                readSchemaRule( recordsBefore );

        return new Command.SchemaRuleCommand().init( recordsBefore, recordsAfter, rule, txId );
    }

    private static SchemaRule readSchemaRule( Collection<DynamicRecord> recordsBefore )
    {
        assert first( recordsBefore ).inUse() : "Asked to deserialize schema records that were not in use.";

        SchemaRule rule;
        ByteBuffer deserialized = AbstractDynamicStore.concatData( recordsBefore, new byte[100] );
        try
        {
            rule = SchemaRule.Kind.deserialize( first( recordsBefore ).getId(), deserialized );
        }
        catch ( MalformedSchemaRuleException e )
        {
            throw new IllegalStateException( e );
        }
        return rule;
    }

    private static final DynamicRecordAdder<Collection<DynamicRecord>> COLLECTION_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<Collection<DynamicRecord>>()
            {
                @Override
                public void add( Collection<DynamicRecord> target, DynamicRecord record )
                {
                    target.add( record );
                }
            };

}
