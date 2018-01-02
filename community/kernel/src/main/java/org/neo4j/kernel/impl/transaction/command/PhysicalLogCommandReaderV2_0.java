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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
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

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.COLLECTION_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.PROPERTY_DELETED_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.PROPERTY_INDEX_DYNAMIC_RECORD_ADDER;

public class PhysicalLogCommandReaderV2_0 implements CommandReader
{
    private final PhysicalNeoCommandReader reader = new PhysicalNeoCommandReader();
    private ReadableLogChannel channel;

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
        case NeoCommandType.NONE:
        {
            command = null;
            break;
        }
        default:
        {
            throw new IOException( "Unknown command type[" + commandType + "]" );
        }
        }
        if ( command != null && command.handle( reader ) )
        {
            return null;
        }
        return command;
    }

    private class PhysicalNeoCommandReader extends CommandHandler.Adapter
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
            byte inUseFlag = channel.get();
            boolean inUse = false;
            if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
            {
                inUse = true;
            }
            else if ( (inUseFlag & Record.IN_USE.byteValue()) != Record.NOT_IN_USE.byteValue() )
            {
                throw new IOException( "Illegal in use flag: " + inUseFlag );
            }
            RelationshipRecord record;
            if ( inUse )
            {
                record = new RelationshipRecord( id, channel.getLong(), channel.getLong(), channel.getInt() );
                record.setInUse( inUse );
                record.setFirstPrevRel( channel.getLong() );
                record.setFirstNextRel( channel.getLong() );
                record.setSecondPrevRel( channel.getLong() );
                record.setSecondNextRel( channel.getLong() );
                record.setNextProp( channel.getLong() );

                /*
                 * Logs for version 2.0 do not contain the proper values for the following two flags. Also,
                 * the defaults won't do, because the pointers for prev in the fist record will not be interpreted
                 * properly. So we need to set the flags explicitly here.
                 *
                 * Note that this leaves the prev field for the first record in the chain having a value of -1,
                 * which is not correct, as it should contain the relationship count instead. However, we cannot
                 * determine this value from the contents of the log alone.
                 */
                record.setFirstInFirstChain( record.getFirstPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() );
                record.setFirstInSecondChain( record.getSecondPrevRel() == Record.NO_PREV_RELATIONSHIP.intValue() );
            }
            else
            {
                record = new RelationshipRecord( id, -1, -1, -1 );
                record.setInUse( false );
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
        public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws
                IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            int id = channel.getInt();
            byte inUseFlag = channel.get();
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
            if ( (inUseFlag & Record.IN_USE.byteValue()) ==
                 Record.IN_USE.byteValue() )
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
            record.setPropertyCount( channel.getInt() );
            record.setNameId( channel.getInt() );
            int recordNr = readDynamicRecords( record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER );
            if ( recordNr == -1 )
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

            // read and ignore transaction id which is not used anymore
            channel.getLong();

            SchemaRule rule = first( recordsAfter ).inUse() ?
                              readSchemaRule( recordsAfter ) :
                              readSchemaRule( recordsBefore );

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

        private NodeRecord readNodeRecord( long id )
                throws IOException
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
                record = new NodeRecord( id, false, channel.getLong(), channel.getLong() );
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
            assert id >= 0 && id <= (1l << 36) - 1 : id
                                                     + " is not a valid dynamic record id";
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
            final int numberOfRecords = channel.getInt();
            assert numberOfRecords >= 0;
            int records = numberOfRecords;
            while ( records-- > 0 )
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


        private PropertyRecord readPropertyRecord( long id )
                throws IOException
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

            if ( readDynamicRecords( record, PROPERTY_DELETED_DYNAMIC_RECORD_ADDER ) == -1 )
            {
                return null;
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

        PropertyBlock readPropertyBlock() throws IOException
        {
            PropertyBlock toReturn = new PropertyBlock();
            byte blockSize = channel.get(); // the size is stored in bytes // 1
            assert blockSize > 0 && blockSize % 8 == 0 : blockSize
                                                         + " is not a valid block size value";
            // Read in blocks
            long[] blocks = readLongs( blockSize / 8 );
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
            assert first( recordsBefore ).inUse() : "Asked to deserialize schema records that were not in use.";

            SchemaRule rule;
            ByteBuffer deserialized = AbstractDynamicStore.concatData( recordsBefore, new byte[100] );
            try
            {
                rule = SchemaRule.Kind.deserialize( first( recordsBefore ).getId(), deserialized );
            }
            catch ( MalformedSchemaRuleException e )
            {
                // TODO This is bad. We should probably just shut down if that happens
                throw launderedException( e );
            }
            return rule;
        }
    }
}
