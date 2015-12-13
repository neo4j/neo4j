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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.DynamicRecordAdder;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.COLLECTION_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.PROPERTY_DELETED_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.PROPERTY_INDEX_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.util.Bits.bitFlag;
import static org.neo4j.kernel.impl.util.Bits.notFlag;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read2bLengthAndString;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read2bMap;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read3bLengthAndString;

public class PhysicalLogCommandReaderV2_2 extends BaseCommandReader
{
    @Override
    protected Command read( byte commandType, ReadableLogChannel channel ) throws IOException
    {
        switch ( commandType )
        {
        case NeoCommandType.NODE_COMMAND: return visitNodeCommand( channel );
        case NeoCommandType.PROP_COMMAND: return visitPropertyCommand( channel );
        case NeoCommandType.PROP_INDEX_COMMAND: return visitPropertyKeyTokenCommand( channel );
        case NeoCommandType.REL_COMMAND: return visitRelationshipCommand( channel );
        case NeoCommandType.REL_TYPE_COMMAND: return visitRelationshipTypeTokenCommand( channel );
        case NeoCommandType.LABEL_KEY_COMMAND: return visitLabelTokenCommand( channel );
        case NeoCommandType.NEOSTORE_COMMAND: return visitNeoStoreCommand( channel );
        case NeoCommandType.SCHEMA_RULE_COMMAND: return visitSchemaRuleCommand( channel );
        case NeoCommandType.REL_GROUP_COMMAND: return visitRelationshipGroupCommand( channel );
        case NeoCommandType.INDEX_DEFINE_COMMAND: return visitIndexDefineCommand( channel );
        case NeoCommandType.INDEX_ADD_COMMAND: return visitIndexAddNodeCommand( channel );
        case NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND: return visitIndexAddRelationshipCommand( channel );
        case NeoCommandType.INDEX_REMOVE_COMMAND: return visitIndexRemoveCommand( channel );
        case NeoCommandType.INDEX_DELETE_COMMAND: return visitIndexDeleteCommand( channel );
        case NeoCommandType.INDEX_CREATE_COMMAND: return visitIndexCreateCommand( channel );
        case NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND: return visitRelationshipCountsCommand( channel );
        case NeoCommandType.UPDATE_NODE_COUNTS_COMMAND: return visitNodeCountsCommand( channel );
        default: throw unknownCommandType( commandType, channel );
        }
    }

    private static final class IndexCommandHeader
    {
        byte valueType;
        byte entityType;
        boolean entityIdNeedsLong;
        byte indexNameId;
        boolean startNodeNeedsLong;
        boolean endNodeNeedsLong;
        byte keyId;

        IndexCommandHeader set( byte valueType, byte entityType, boolean entityIdNeedsLong,
                                byte indexNameId, boolean startNodeNeedsLong, boolean endNodeNeedsLong, byte keyId )
        {
            this.valueType = valueType;
            this.entityType = entityType;
            this.entityIdNeedsLong = entityIdNeedsLong;
            this.indexNameId = indexNameId;
            this.startNodeNeedsLong = startNodeNeedsLong;
            this.endNodeNeedsLong = endNodeNeedsLong;
            this.keyId = keyId;
            return this;
        }
    }

    private Command visitNodeCommand( ReadableLogChannel channel ) throws IOException
    {
        long id = channel.getLong();
        NodeRecord before = readNodeRecord( id, channel );
        if ( before == null )
        {
            return null;
        }
        NodeRecord after = readNodeRecord( id, channel );
        if ( after == null )
        {
            return null;
        }
        if ( !before.inUse() && after.inUse() )
        {
            after.setCreated();
        }
        Command.NodeCommand command = new Command.NodeCommand();
        command.init( before, after );
        return command;
    }

    private Command visitRelationshipCommand( ReadableLogChannel channel ) throws IOException
    {
        long id = channel.getLong();
        byte flags = channel.get();
        boolean inUse = false;
        if ( notFlag( notFlag( flags, Record.IN_USE.byteValue() ), Record.CREATED_IN_TX ) != 0 )
        {
            throw new IOException( "Illegal in use flag: " + flags );
        }
        if ( bitFlag( flags, Record.IN_USE.byteValue() ) )
        {
            inUse = true;
        }
        RelationshipRecord record;
        if ( inUse )
        {
            record = new RelationshipRecord( id, channel.getLong(), channel.getLong(), channel.getInt() );
            record.setInUse( true );
            record.setFirstPrevRel( channel.getLong() );
            record.setFirstNextRel( channel.getLong() );
            record.setSecondPrevRel( channel.getLong() );
            record.setSecondNextRel( channel.getLong() );
            record.setNextProp( channel.getLong() );
            byte extraByte = channel.get();
            record.setFirstInFirstChain( (extraByte & 0x1) > 0 );
            record.setFirstInSecondChain( (extraByte & 0x2) > 0 );
        }
        else
        {
            record = new RelationshipRecord( id, -1, -1, channel.getInt() );
            record.setInUse( false );
        }
        if ( bitFlag( flags, Record.CREATED_IN_TX ) )
        {
            record.setCreated();
        }
        Command.RelationshipCommand command = new Command.RelationshipCommand();
        command.init( record );
        return command;
    }

    private Command visitPropertyCommand( ReadableLogChannel channel ) throws IOException
    {
        // ID
        long id = channel.getLong(); // 8
        // BEFORE
        PropertyRecord before = readPropertyRecord( id, channel );
        if ( before == null )
        {
            return null;
        }
        // AFTER
        PropertyRecord after = readPropertyRecord( id, channel );
        if ( after == null )
        {
            return null;
        }
        Command.PropertyCommand command = new Command.PropertyCommand();
        command.init( before, after );
        return command;
    }

    private Command visitRelationshipGroupCommand( ReadableLogChannel channel ) throws IOException
    {
        long id = channel.getLong();
        byte inUseByte = channel.get();
        boolean inUse = inUseByte == Record.IN_USE.byteValue();
        if ( inUseByte != Record.IN_USE.byteValue() && inUseByte != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseByte );
        }
        int type = channel.getShort();
        RelationshipGroupRecord record = new RelationshipGroupRecord( id, type );
        record.setInUse( inUse );
        record.setNext( channel.getLong() );
        record.setFirstOut( channel.getLong() );
        record.setFirstIn( channel.getLong() );
        record.setFirstLoop( channel.getLong() );
        record.setOwningNode( channel.getLong() );
        Command.RelationshipGroupCommand command = new Command.RelationshipGroupCommand();
        command.init( record );
        return command;
    }

    private Command visitRelationshipTypeTokenCommand( ReadableLogChannel channel ) throws IOException
    {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        int id = channel.getInt();
        byte inUseFlag = channel.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        record.setInUse( inUse );
        record.setNameId( channel.getInt() );
        int nrTypeRecords = channel.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( channel );
            if ( dr == null )
            {
                return null;
            }
            record.addNameRecord( dr );
        }
        Command.RelationshipTypeTokenCommand command = new Command.RelationshipTypeTokenCommand();
        command.init( record );
        return command;
    }

    private Command visitLabelTokenCommand( ReadableLogChannel channel ) throws IOException
    {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        int id = channel.getInt();
        byte inUseFlag = channel.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        LabelTokenRecord record = new LabelTokenRecord( id );
        record.setInUse( inUse );
        record.setNameId( channel.getInt() );
        int nrTypeRecords = channel.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( channel );
            if ( dr == null )
            {
                return null;
            }
            record.addNameRecord( dr );
        }
        Command.LabelTokenCommand command = new Command.LabelTokenCommand();
        command.init( record );
        return command;
    }

    private Command visitPropertyKeyTokenCommand( ReadableLogChannel channel ) throws IOException
    {
        // id+in_use(byte)+count(int)+key_blockId(int)
        int id = channel.getInt();
        byte inUseFlag = channel.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( channel.getInt() );
        record.setNameId( channel.getInt() );
        if ( readDynamicRecords( channel, record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER ) == -1 )
        {
            return null;
        }
        Command.PropertyKeyTokenCommand command = new Command.PropertyKeyTokenCommand();
        command.init( record );
        return command;
    }

    private Command visitSchemaRuleCommand( ReadableLogChannel channel ) throws IOException
    {
        Collection<DynamicRecord> recordsBefore = new ArrayList<>();
        readDynamicRecords( channel, recordsBefore, COLLECTION_DYNAMIC_RECORD_ADDER );
        Collection<DynamicRecord> recordsAfter = new ArrayList<>();
        readDynamicRecords( channel, recordsAfter, COLLECTION_DYNAMIC_RECORD_ADDER );
        byte isCreated = channel.get();
        if ( 1 == isCreated )
        {
            for ( DynamicRecord record : recordsAfter )
            {
                record.setCreated();
            }
        }
        SchemaRule rule = first( recordsAfter ).inUse() ? readSchemaRule( recordsAfter )
                                                        : readSchemaRule( recordsBefore );
        Command.SchemaRuleCommand command = new Command.SchemaRuleCommand();
        command.init( recordsBefore, recordsAfter, rule );
        return command;
    }

    private Command visitNeoStoreCommand( ReadableLogChannel channel ) throws IOException
    {
        long nextProp = channel.getLong();
        NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( nextProp );
        Command.NeoStoreCommand command = new Command.NeoStoreCommand();
        command.init( record );
        return command;
    }

    private NodeRecord readNodeRecord( long id, ReadableLogChannel channel ) throws IOException
    {
        byte inUseFlag = channel.get();
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
        Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<>();
        long labelField = Record.NO_LABELS_FIELD.intValue();
        if ( inUse )
        {
            boolean dense = channel.get() == 1;
            record = new NodeRecord( id, dense, channel.getLong(), channel.getLong() );
            // labels
            labelField = channel.getLong();
        }
        else
        {
            record = new NodeRecord( id, false, Record.NO_NEXT_RELATIONSHIP.intValue(),
                    Record.NO_NEXT_PROPERTY.intValue() );
        }

        readDynamicRecords( channel, dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
        record.setLabelField( labelField, dynamicLabelRecords );

        record.setInUse( inUse );
        return record;
    }

    private DynamicRecord readDynamicRecord( ReadableLogChannel channel ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        long id = channel.getLong();
        assert id >= 0 && id <= (1l << 36) - 1 : id + " is not a valid dynamic record id";
        int type = channel.getInt();
        byte inUseFlag = channel.get();
        boolean inUse = (inUseFlag & Record.IN_USE.byteValue()) != 0;
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            record.setStartRecord( (inUseFlag & Record.FIRST_IN_CHAIN.byteValue()) != 0 );
            int nrOfBytes = channel.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ((1 << 24) - 1) : nrOfBytes
                                                                   +
                                                                   " is not valid for a number of bytes field of " +
                                                                   "a dynamic record";
            long nextBlock = channel.getLong();
            assert (nextBlock >= 0 && nextBlock <= (1l << 36 - 1))
                   || (nextBlock == Record.NO_NEXT_BLOCK.intValue()) : nextBlock
                                                                       +
                                                                       " is not valid for a next record field of " +
                                                                       "a dynamic record";
            record.setNextBlock( nextBlock );
            byte data[] = new byte[nrOfBytes];
            channel.get( data, nrOfBytes );
            record.setData( data );
        }
        return record;
    }

    private <T> int readDynamicRecords( ReadableLogChannel channel, T target, DynamicRecordAdder<T> adder )
            throws IOException
    {
        int numberOfRecords = channel.getInt();
        assert numberOfRecords >= 0;
        while ( numberOfRecords > 0 )
        {
            DynamicRecord read = readDynamicRecord( channel );
            if ( read == null )
            {
                return -1;
            }
            adder.add( target, read );
            numberOfRecords--;
        }
        return numberOfRecords;
    }

    private PropertyRecord readPropertyRecord( long id, ReadableLogChannel channel ) throws IOException
    {
        // in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
        // prev_prop_id(long)+next_prop_id(long)
        PropertyRecord record = new PropertyRecord( id );
        byte inUseFlag = channel.get(); // 1
        long nextProp = channel.getLong(); // 8
        long prevProp = channel.getLong(); // 8
        record.setNextProp( nextProp );
        record.setPrevProp( prevProp );
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        boolean nodeProperty = true;
        if ( (inUseFlag & Record.REL_PROPERTY.byteValue()) == Record.REL_PROPERTY.byteValue() )
        {
            nodeProperty = false;
        }
        long primitiveId = channel.getLong(); // 8
        if ( primitiveId != -1 && nodeProperty )
        {
            record.setNodeId( primitiveId );
        }
        else if ( primitiveId != -1 )
        {
            record.setRelId( primitiveId );
        }
        int nrPropBlocks = channel.get();
        assert nrPropBlocks >= 0;
        if ( nrPropBlocks > 0 )
        {
            record.setInUse( true );
        }
        while ( nrPropBlocks-- > 0 )
        {
            PropertyBlock block = readPropertyBlock( channel );
            if ( block == null )
            {
                return null;
            }
            record.addPropertyBlock( block );
        }
        int deletedRecords = readDynamicRecords( channel, record, PROPERTY_DELETED_DYNAMIC_RECORD_ADDER );
        if ( deletedRecords == -1 )
        {
            return null;
        }
        assert deletedRecords >= 0;
        while ( deletedRecords-- > 0 )
        {
            DynamicRecord read = readDynamicRecord( channel );
            if ( read == null )
            {
                return null;
            }
            record.addDeletedRecord( read );
        }
        if ( (inUse && !record.inUse()) || (!inUse && record.inUse()) )
        {
            throw new IllegalStateException( "Weird, inUse was read in as " + inUse + " but the record is "
                                             + record );
        }
        return record;
    }

    private PropertyBlock readPropertyBlock( ReadableLogChannel channel ) throws IOException
    {
        PropertyBlock toReturn = new PropertyBlock();
        byte blockSize = channel.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize + " is not a valid block size value";
        // Read in blocks
        long[] blocks = readLongs( channel, blockSize / 8 );
        assert blocks.length == blockSize / 8 : blocks.length
                                                + " longs were read in while i asked for what corresponds to " +
                                                blockSize;
        assert PropertyType.getPropertyType( blocks[0], false ).calculateNumberOfBlocksUsed( blocks[0] ) ==
               blocks.length : blocks.length
                               + " is not a valid number of blocks for type " +
                               PropertyType.getPropertyType( blocks[0], false );
        /*
         *  Ok, now we may be ready to return, if there are no DynamicRecords. So
         *  we start building the Object
         */
        toReturn.setValueBlocks( blocks );
        /*
         * Read in existence of DynamicRecords. Remember, this has already been
         * read in the buffer with the blocks, above.
         */
        if ( readDynamicRecords( channel, toReturn, PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER ) == -1 )
        {
            return null;
        }
        return toReturn;
    }

    private long[] readLongs( ReadableLogChannel channel, int count ) throws IOException
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = channel.getLong();
        }
        return result;
    }

    private SchemaRule readSchemaRule( Collection<DynamicRecord> recordsBefore )
    {
        // TODO: Why was this assertion here?
        //            assert first(recordsBefore).inUse() : "Asked to deserialize schema records that were not in
        // use.";
        SchemaRule rule;
        ByteBuffer deserialized = AbstractDynamicStore.concatData( recordsBefore, new byte[100] );
        try
        {
            rule = SchemaRule.Kind.deserialize( first( recordsBefore ).getId(), deserialized );
        }
        catch ( MalformedSchemaRuleException e )
        {
            return null;
        }
        return rule;
    }

    private Command visitIndexAddNodeCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        AddNodeCommand command = new AddNodeCommand();
        command.init( header.indexNameId, entityId.longValue(), header.keyId, value );
        return command;
    }

    private Command visitIndexAddRelationshipCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        Number startNode = header.startNodeNeedsLong ? channel.getLong() : channel.getInt();
        Number endNode = header.endNodeNeedsLong ? channel.getLong() : channel.getInt();
        AddRelationshipCommand command = new AddRelationshipCommand();
        command.init( header.indexNameId, entityId.longValue(), header.keyId, value,
                startNode.longValue(), endNode.longValue() );
        return command;
    }

    private Command visitIndexRemoveCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        RemoveCommand command = new RemoveCommand();
        command.init( header.indexNameId, header.entityType, entityId.longValue(), header.keyId, value );
        return command;
    }

    private Command visitIndexDeleteCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        DeleteCommand command = new DeleteCommand();
        command.init( header.indexNameId, header.entityType );
        return command;
    }

    private Command visitIndexCreateCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Map<String,String> config = read2bMap( channel );
        CreateCommand command = new CreateCommand();
        command.init( header.indexNameId, header.entityType, config );
        return command;
    }

    private Command visitIndexDefineCommand( ReadableLogChannel channel ) throws IOException
    {
        readIndexCommandHeader( channel );
        Map<String,Integer> indexNames = readMap( channel );
        Map<String,Integer> keys = readMap( channel );
        IndexDefineCommand command = new IndexDefineCommand();
        command.init( indexNames, keys );
        return command;
    }

    private Command visitNodeCountsCommand( ReadableLogChannel channel ) throws IOException
    {
        int labelId = channel.getInt();
        long delta = channel.getLong();
        Command.NodeCountsCommand command = new Command.NodeCountsCommand();
        command.init( labelId, delta );
        return command;
    }

    private Command visitRelationshipCountsCommand( ReadableLogChannel channel ) throws IOException
    {
        int startLabelId = channel.getInt();
        int typeId = channel.getInt();
        int endLabelId = channel.getInt();
        long delta = channel.getLong();
        Command.RelationshipCountsCommand command = new Command.RelationshipCountsCommand();
        command.init( startLabelId, typeId, endLabelId, delta );
        return command;
    }

    private Map<String,Integer> readMap( ReadableLogChannel channel ) throws IOException
    {
        byte size = channel.get();
        Map<String,Integer> result = new HashMap<>();
        for ( int i = 0; i < size; i++ )
        {
            String key = read2bLengthAndString( channel );
            int id = channel.get();
            if ( key == null )
            {
                return null;
            }
            result.put( key, id );
        }
        return result;
    }

    private IndexCommandHeader readIndexCommandHeader( ReadableLogChannel channel ) throws IOException
    {
        byte[] headerBytes = new byte[3];
        channel.get( headerBytes, headerBytes.length );
        byte valueType = (byte) ((headerBytes[0] & 0x1C) >> 2);
        byte entityType = (byte) ((headerBytes[0] & 0x2) >> 1);
        boolean entityIdNeedsLong = (headerBytes[0] & 0x1) > 0;
        byte indexNameId = (byte) (headerBytes[1] & 0x3F);

        boolean startNodeNeedsLong = (headerBytes[1] & 0x80) > 0;
        boolean endNodeNeedsLong = (headerBytes[1] & 0x40) > 0;

        byte keyId = headerBytes[2];
        return new IndexCommandHeader().set( valueType, entityType, entityIdNeedsLong,
                indexNameId, startNodeNeedsLong, endNodeNeedsLong, keyId );
    }

    private Object readIndexValue( byte valueType, ReadableLogChannel channel ) throws IOException
    {
        switch ( valueType )
        {
        case IndexCommand.VALUE_TYPE_NULL:
            return null;
        case IndexCommand.VALUE_TYPE_SHORT:
            return channel.getShort();
        case IndexCommand.VALUE_TYPE_INT:
            return channel.getInt();
        case IndexCommand.VALUE_TYPE_LONG:
            return channel.getLong();
        case IndexCommand.VALUE_TYPE_FLOAT:
            return channel.getFloat();
        case IndexCommand.VALUE_TYPE_DOUBLE:
            return channel.getDouble();
        case IndexCommand.VALUE_TYPE_STRING:
            return read3bLengthAndString( channel );
        default:
            throw new RuntimeException( "Unknown value type " + valueType );
        }
    }
}
