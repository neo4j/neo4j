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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandType;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandVisitor;

import static org.neo4j.helpers.collection.IteratorUtil.first;

public class CommandSerializer extends NeoCommandVisitor.Adapter
{
    private final WritableLogChannel channel;

    public CommandSerializer( WritableLogChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        NodeRecord before = command.getBefore();
        NodeRecord after = command.getAfter();

        channel.put( NeoCommandType.NODE_COMMAND );
        channel.putLong( after.getId() );

        writeNodeRecord( before );
        writeNodeRecord( after );
        return true;
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
    {
        RelationshipRecord record = command.getRecord();
        byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
        channel.put( NeoCommandType.REL_COMMAND );
        channel.putLong( record.getId() );
        channel.put( inUse );
        if ( record.inUse() )
        {
            channel.putLong( record.getFirstNode() )
                    .putLong( record.getSecondNode() )
                    .putInt( record.getType() )
                    .putLong( record.getFirstPrevRel() )
                    .putLong( record.getFirstNextRel() )
                    .putLong( record.getSecondPrevRel() )
                    .putLong( record.getSecondNextRel() )
                    .putLong( record.getNextProp() )
                    .put( (byte) ((record.isFirstInFirstChain() ? 1 : 0) | (record.isFirstInSecondChain() ? 2 : 0)) )
            ;
        }
        return true;
    }

    private boolean writeNodeRecord( NodeRecord record ) throws IOException
    {
        byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
        channel.put( inUse );
        if ( record.inUse() )
        {
            channel.put( record.isDense() ? (byte)1 : (byte)0 );
            channel.putLong( record.getNextRel() ).putLong( record.getNextProp() );

            // labels
            channel.putLong( record.getLabelField() );
            writeDynamicRecords( record.getDynamicLabelRecords() );
        }
        return true;
    }

    @Override
    public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
    {
        // COMMAND + ID
        channel.put( NeoCommandType.PROP_COMMAND );
        channel.putLong( command.getKey() ); // 8

        // BEFORE
        writePropertyRecord( command.getBefore() );

        // AFTER
        writePropertyRecord( command.getAfter() );
        return true;
    }

    @Override
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
    {
        RelationshipGroupRecord record = command.getRecord();
        channel.put( NeoCommandType.REL_GROUP_COMMAND );
        channel.putLong( record.getId() );
        channel.put( (byte) (record.inUse() ? Record.IN_USE.intValue() : Record.NOT_IN_USE.intValue()) );
        channel.putShort( (short) record.getType() );
        channel.putLong( record.getNext() );
        channel.putLong( record.getFirstOut() );
        channel.putLong( record.getFirstIn() );
        channel.putLong( record.getFirstLoop() );
        channel.putLong( record.getOwningNode() );
        return true;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException
    {
        RelationshipTypeTokenRecord record = command.getRecord();
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
        channel.put( NeoCommandType.REL_TYPE_COMMAND );
        channel.putInt( record.getId() ).put( inUse ).putInt( record.getNameId() );
        writeDynamicRecords( record.getNameRecords() );
        return true;
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
    {
        LabelTokenRecord record = command.getRecord();
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
        channel.put( NeoCommandType.LABEL_KEY_COMMAND );
        channel.putInt( record.getId() ).put( inUse ).putInt( record.getNameId() );
        writeDynamicRecords( record.getNameRecords() );
        return true;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
    {
        PropertyKeyTokenRecord record = command.getRecord();
        // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
        byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
        channel.put( NeoCommandType.PROP_INDEX_COMMAND );
        channel.putInt( record.getId() );
        channel.put( inUse );
        channel.putInt( record.getPropertyCount() ).putInt( record.getNameId() );
        if ( record.isLight() )
        {
            channel.putInt( 0 );
        }
        else
        {
            writeDynamicRecords( record.getNameRecords() );
        }
        return true;
    }

    @Override
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
    {
        Collection<DynamicRecord> recordsAfter = command.getRecordsAfter();
        channel.put( NeoCommandType.SCHEMA_RULE_COMMAND );
        writeDynamicRecords( command.getRecordsBefore() );
        writeDynamicRecords( recordsAfter );
        channel.put( first( recordsAfter ).isCreated() ? (byte) 1 : 0);
        return true;
    }

    @Override
    public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
    {
        channel.put( NeoCommandType.NEOSTORE_COMMAND ).putLong( command.getRecord().getNextProp() );
        return true;
    }

    void writeDynamicRecords( Collection<DynamicRecord> records ) throws IOException
    {
        channel.putInt( records.size() ); // 4
        for ( DynamicRecord record : records )
        {
            writeDynamicRecord( record );
        }
    }

    void writeDynamicRecord( DynamicRecord record )
            throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            if ( record.isStartRecord() )
            {
                inUse |= Record.FIRST_IN_CHAIN.byteValue();
            }
            channel.putLong( record.getId() ).putInt( record.getType() ).put(
                    inUse ).putInt( record.getLength() ).putLong(
                    record.getNextBlock() );
            byte[] data = record.getData();
            assert data != null;
            channel.put( data, data.length );
        }
        else
        {
            byte inUse = Record.NOT_IN_USE.byteValue();
            channel.putLong( record.getId() ).putInt( record.getType() ).put(
                    inUse );
        }
    }

    private void writePropertyBlock( PropertyBlock block ) throws IOException
    {
        byte blockSize = (byte) block.getSize();
        assert blockSize > 0 : blockSize + " is not a valid block size value";
        channel.put( blockSize ); // 1
        long[] propBlockValues = block.getValueBlocks();
        for ( long propBlockValue : propBlockValues )
        {
            channel.putLong( propBlockValue );
        }
        /*
         * For each block we need to keep its dynamic record chain if
         * it is just created. Deleted dynamic records are in the property
         * record and dynamic records are never modified. Also, they are
         * assigned as a whole, so just checking the first should be enough.
         */
        if ( block.isLight() )
        {
            /*
             *  This has to be int. If this record is not light
             *  then we have the number of DynamicRecords that follow,
             *  which is an int. We do not currently want/have a flag bit so
             *  we simplify by putting an int here always
             */
            channel.putInt( 0 ); // 4 or
        }
        else
        {
            writeDynamicRecords( block.getValueRecords() );
        }
    }

    private void writePropertyRecord( PropertyRecord record ) throws IOException
    {
        byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
        if ( record.getRelId() != -1 )
        {
            // Here we add 2, i.e. set the second lsb.
            inUse += Record.REL_PROPERTY.byteValue();
        }
        channel.put( inUse ); // 1
        channel.putLong( record.getNextProp() ).putLong(
                record.getPrevProp() ); // 8 + 8
        long nodeId = record.getNodeId();
        long relId = record.getRelId();
        if ( nodeId != -1 )
        {
            channel.putLong( nodeId ); // 8 or
        }
        else if ( relId != -1 )
        {
            channel.putLong( relId ); // 8 or
        }
        else
        {
            // means this records value has not changed, only place in
            // prop chain
            channel.putLong( -1 ); // 8
        }
        channel.put( (byte) record.getPropertyBlocks().size() ); // 1
        for ( int i = 0; i < record.getPropertyBlocks().size(); i++ )
        {
            PropertyBlock block = record.getPropertyBlocks().get( i );
            assert block.getSize() > 0 : record + " seems kinda broken";
            writePropertyBlock( block );
        }
        writeDynamicRecords( record.getDeletedRecords() );
    }
}
