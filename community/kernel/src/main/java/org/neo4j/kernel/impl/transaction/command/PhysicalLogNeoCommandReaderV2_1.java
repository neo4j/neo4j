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
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCountsCommand;
import org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.DynamicRecordAdder;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadPastEndException;
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

public class PhysicalLogNeoCommandReaderV2_1 implements CommandReader
{
    private final PhysicalNeoCommandReader reader = new PhysicalNeoCommandReader();
    private ReadableLogChannel channel;
    private IndexCommandHeader indexCommandHeader;

    @Override
    public Command read( ReadableLogChannel channel ) throws IOException
    {
        // for the reader to pick up
        this.channel = channel;
        byte commandType = 0;
        while ( commandType == 0 )
        {
            commandType = channel.get();
        }
        Command command;
        switch ( commandType )
        {
        case NeoCommandType.NODE_COMMAND:
        {
            command = new Command.NodeCommand();
            break;
        }
        case NeoCommandType.PROP_COMMAND:
        {
            command = new Command.PropertyCommand();
            break;
        }
        case NeoCommandType.PROP_INDEX_COMMAND:
        {
            command = new Command.PropertyKeyTokenCommand();
            break;
        }
        case NeoCommandType.REL_COMMAND:
        {
            command = new Command.RelationshipCommand();
            break;
        }
        case NeoCommandType.REL_TYPE_COMMAND:
        {
            command = new Command.RelationshipTypeTokenCommand();
            break;
        }
        case NeoCommandType.LABEL_KEY_COMMAND:
        {
            command = new Command.LabelTokenCommand();
            break;
        }
        case NeoCommandType.NEOSTORE_COMMAND:
        {
            command = new Command.NeoStoreCommand();
            break;
        }
        case NeoCommandType.SCHEMA_RULE_COMMAND:
        {
            command = new Command.SchemaRuleCommand();
            break;
        }
        case NeoCommandType.REL_GROUP_COMMAND:
        {
            command = new Command.RelationshipGroupCommand();
            break;
        }
        case NeoCommandType.INDEX_DEFINE_COMMAND:
        {
            command = new IndexDefineCommand();
            break;
        }
        case NeoCommandType.INDEX_ADD_COMMAND:
        {
            command = new IndexCommand.AddNodeCommand();
            break;
        }
        case NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND:
        {
            command = new IndexCommand.AddRelationshipCommand();
            break;
        }
        case NeoCommandType.INDEX_REMOVE_COMMAND:
        {
            command = new IndexCommand.RemoveCommand();
            break;
        }
        case NeoCommandType.INDEX_DELETE_COMMAND:
        {
            command = new IndexCommand.DeleteCommand();
            break;
        }
        case NeoCommandType.INDEX_CREATE_COMMAND:
        {
            command = new IndexCommand.CreateCommand();
            break;
        }
        case NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND:
        {
            command = new RelationshipCountsCommand();
            break;
        }
        case NeoCommandType.UPDATE_NODE_COUNTS_COMMAND:
        {
            command = new NodeCountsCommand();
            break;
        }
        case NeoCommandType.NONE:
        {
            command = null;
            break;
        }
        default:
        {
            LogPositionMarker position = new LogPositionMarker();
            channel.getCurrentPosition( position );
            throw new IOException( "Unknown command type[" + commandType + "] near " + position.newPosition() );
        }
        }
        if ( command != null && command.handle( reader ) )
        {
            return null;
        }
        return command;
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

    private class PhysicalNeoCommandReader implements NeoCommandHandler
    {
        @Override
        public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
        {
            long id = channel.getLong();
            NodeRecord before = readNodeRecord( id );
            if ( before == null )
            {
                return true;
            }
            NodeRecord after = readNodeRecord( id );
            if ( after == null )
            {
                return true;
            }
            if ( !before.inUse() && after.inUse() )
            {
                after.setCreated();
            }
            command.init( before, after );
            return false;
        }

        @Override
        public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
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
                record = new RelationshipRecord( id, -1, -1, -1 );
                record.setInUse( false );
            }
            if ( bitFlag( flags, Record.CREATED_IN_TX ) )
            {
                record.setCreated();
            }
            command.init( record );
            return false;
        }

        @Override
        public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
        {
            // ID
            long id = channel.getLong(); // 8
            // BEFORE
            PropertyRecord before = readPropertyRecord( id );
            if ( before == null )
            {
                return true;
            }
            // AFTER
            PropertyRecord after = readPropertyRecord( id );
            if ( after == null )
            {
                return true;
            }
            command.init( before, after );
            return false;
        }

        @Override
        public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
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
            command.init( record );
            return false;
        }

        @Override
        public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command )
                throws IOException
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
                DynamicRecord dr = readDynamicRecord();
                if ( dr == null )
                {
                    return true;
                }
                record.addNameRecord( dr );
            }
            command.init( record );
            return false;
        }

        @Override
        public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
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
                DynamicRecord dr = readDynamicRecord();
                if ( dr == null )
                {
                    return true;
                }
                record.addNameRecord( dr );
            }
            command.init( record );
            return false;
        }

        @Override
        public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
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
            if ( readDynamicRecords( record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER ) == -1 )
            {
                return true;
            }
            command.init( record );
            return false;
        }

        @Override
        public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
        {
            Collection<DynamicRecord> recordsBefore = new ArrayList<>();
            readDynamicRecords( recordsBefore, COLLECTION_DYNAMIC_RECORD_ADDER );
            Collection<DynamicRecord> recordsAfter = new ArrayList<>();
            readDynamicRecords( recordsAfter, COLLECTION_DYNAMIC_RECORD_ADDER );
            byte isCreated = channel.get();
            if ( 1 == isCreated )
            {
                for ( DynamicRecord record : recordsAfter )
                {
                    record.setCreated();
                }
            }

            channel.getLong(); // txId - ignored

            SchemaRule rule = first( recordsAfter ).inUse() ? readSchemaRule( recordsAfter )
                                                            : readSchemaRule( recordsBefore );
            command.init( recordsBefore, recordsAfter, rule );
            return false;
        }

        @Override
        public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
        {
            long nextProp = channel.getLong();
            NeoStoreRecord record = new NeoStoreRecord();
            record.setNextProp( nextProp );
            command.init( record );
            return false;
        }

        private NodeRecord readNodeRecord( long id ) throws IOException
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
            if ( inUse )
            {
                boolean dense = channel.get() == 1;
                record = new NodeRecord( id, dense, channel.getLong(), channel.getLong() );
                // labels
                long labelField = channel.getLong();
                Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<>();
                readDynamicRecords( dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
                record.setLabelField( labelField, dynamicLabelRecords );
            }
            else
            {
                record = new NodeRecord( id, false, Record.NO_NEXT_RELATIONSHIP.intValue(),
                        Record.NO_NEXT_PROPERTY.intValue() );
            }
            record.setInUse( inUse );
            return record;
        }

        DynamicRecord readDynamicRecord() throws IOException
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

        <T> int readDynamicRecords( T target, DynamicRecordAdder<T> adder ) throws IOException
        {
            int numberOfRecords = channel.getInt();
            assert numberOfRecords >= 0;
            for  ( int i = numberOfRecords; i > 0; i-- )
            {
                DynamicRecord read = readDynamicRecord();
                if ( read == null )
                {
                    return -1;
                }
                adder.add( target, read );
            }
            return numberOfRecords;
        }

        private PropertyRecord readPropertyRecord( long id ) throws IOException
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
                PropertyBlock block = readPropertyBlock();
                if ( block == null )
                {
                    return null;
                }
                record.addPropertyBlock( block );
            }
            int deletedRecords = readDynamicRecords( record, PROPERTY_DELETED_DYNAMIC_RECORD_ADDER );
            if ( deletedRecords == -1 )
            {
                return null;
            }
            if ( (inUse && !record.inUse()) || (!inUse && record.inUse()) )
            {
                throw new IllegalStateException( "Weird, inUse was read in as " + inUse + " but the record is "
                                                 + record );
            }
            return record;
        }

        PropertyBlock readPropertyBlock() throws IOException
        {
            PropertyBlock toReturn = new PropertyBlock();
            byte blockSize = channel.get(); // the size is stored in bytes // 1
            assert blockSize > 0 && blockSize % 8 == 0 : blockSize + " is not a valid block size value";
            // Read in blocks
            long[] blocks = readLongs( blockSize / 8 );
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
            if ( readDynamicRecords( toReturn, PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER ) == -1 )
            {
                return null;
            }
            return toReturn;
        }

        private long[] readLongs( int count ) throws IOException
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

        @Override
        public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
        {
            IndexCommandHeader header = readIndexCommandHeader();
            Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
            Object value = readIndexValue( header.valueType );
            command.init( header.indexNameId, entityId.longValue(), header.keyId, value );
            return false;
        }

        @Override
        public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
        {
            IndexCommandHeader header = readIndexCommandHeader();
            Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
            Object value = readIndexValue( header.valueType );
            Number startNode = header.startNodeNeedsLong ? channel.getLong() : channel.getInt();
            Number endNode = header.endNodeNeedsLong ? channel.getLong() : channel.getInt();
            command.init( header.indexNameId, entityId.longValue(), header.keyId, value,
                    startNode.longValue(), endNode.longValue() );
            return false;
        }

        @Override
        public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
        {
            IndexCommandHeader header = readIndexCommandHeader();
            Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
            Object value = readIndexValue( header.valueType );
            command.init( header.indexNameId, header.entityType, entityId.longValue(), header.keyId, value );
            return false;
        }

        @Override
        public boolean visitIndexDeleteCommand( DeleteCommand deleteCommand ) throws IOException
        {
            IndexCommandHeader header = readIndexCommandHeader();
            deleteCommand.init( header.indexNameId, header.entityType );
            return false;
        }

        @Override
        public boolean visitIndexCreateCommand( CreateCommand createCommand ) throws IOException
        {
            IndexCommandHeader header = readIndexCommandHeader();
            Map<String,String> config = read2bMap( channel );
            createCommand.init( header.indexNameId, header.entityType, config );
            return false;
        }

        @Override
        public boolean visitIndexDefineCommand( IndexDefineCommand indexDefineCommand ) throws IOException
        {
            readIndexCommandHeader();
            Map<String,Integer> indexNames = readMap( channel );
            Map<String,Integer> keys = readMap( channel );
            indexDefineCommand.init( indexNames, keys );
            return false;
        }

        @Override
        public boolean visitNodeCountsCommand( NodeCountsCommand command ) throws IOException
        {
            int labelId = channel.getInt();
            long delta = channel.getLong();
            command.init( labelId, delta );
            return false;
        }

        @Override
        public boolean visitRelationshipCountsCommand( RelationshipCountsCommand command ) throws IOException
        {
            int startLabelId = channel.getInt();
            int typeId = channel.getInt();
            int endLabelId = channel.getInt();
            long delta = channel.getLong();
            command.init( startLabelId, typeId, endLabelId, delta );
            return false;
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

        private IndexCommandHeader readIndexCommandHeader() throws ReadPastEndException, IOException
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
            if ( indexCommandHeader == null )
            {
                indexCommandHeader = new IndexCommandHeader();
            }
            return indexCommandHeader.set( valueType, entityType, entityIdNeedsLong,
                    indexNameId, startNodeNeedsLong, endNodeNeedsLong, keyId );
        }

        private Object readIndexValue( byte valueType ) throws IOException
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

        @Override
        public void apply()
        {   // Nothing to apply
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    }
}
