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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCountsCommand;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.command.NeoCommandType;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.util.Bits.bitFlag;
import static org.neo4j.kernel.impl.util.Bits.bitFlags;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write2bLengthAndString;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write3bLengthAndString;

public class CommandWriter implements NeoCommandHandler
{
    private final WritableLogChannel channel;

    public CommandWriter( WritableLogChannel channel )
    {
        this.channel = channel;
    }

    protected static byte needsLong( long value )
    {
        return value > Integer.MAX_VALUE ? (byte) 1 : (byte) 0;
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
        return false;
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
    {
        RelationshipRecord record = command.getRecord();
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                bitFlag( record.isCreated(), Record.CREATED_IN_TX ) );
        channel.put( NeoCommandType.REL_COMMAND );
        channel.putLong( record.getId() );
        channel.put( flags );
        if ( record.inUse() )
        {
            channel.putLong( record.getFirstNode() ).putLong( record.getSecondNode() ).putInt( record.getType() )
                   .putLong( record.getFirstPrevRel() ).putLong( record.getFirstNextRel() )
                   .putLong( record.getSecondPrevRel() ).putLong( record.getSecondNextRel() )
                   .putLong( record.getNextProp() )
                   .put( (byte) ((record.isFirstInFirstChain() ? 1 : 0) | (record.isFirstInSecondChain() ? 2 : 0)) );
        }
        else
        {
            channel.putInt( record.getType() );
        }
        return false;
    }

    private boolean writeNodeRecord( NodeRecord record ) throws IOException
    {
        byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
        channel.put( inUse );
        if ( record.inUse() )
        {
            channel.put( record.isDense() ? (byte) 1 : (byte) 0 );
            channel.putLong( record.getNextRel() ).putLong( record.getNextProp() );
            channel.putLong( record.getLabelField() );
        }
        // Always write dynamic label records because we want to know which ones have been deleted
        // especially if the node has been deleted.
        writeDynamicRecords( record.getDynamicLabelRecords() );
        return false;
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
        return false;
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
        return false;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException
    {
        RelationshipTypeTokenRecord record = command.getRecord();
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
        channel.put( NeoCommandType.REL_TYPE_COMMAND );
        channel.putInt( record.getId() ).put( inUse ).putInt( record.getNameId() );
        writeDynamicRecords( record.getNameRecords() );
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
    {
        LabelTokenRecord record = command.getRecord();
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
        channel.put( NeoCommandType.LABEL_KEY_COMMAND );
        channel.putInt( record.getId() ).put( inUse ).putInt( record.getNameId() );
        writeDynamicRecords( record.getNameRecords() );
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
    {
        PropertyKeyTokenRecord record = command.getRecord();
        // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
        byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
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
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
    {
        Collection<DynamicRecord> recordsBefore = command.getRecordsBefore();
        Collection<DynamicRecord> recordsAfter = command.getRecordsAfter();

        channel.put( NeoCommandType.SCHEMA_RULE_COMMAND );
        writeDynamicRecords( recordsBefore );
        writeDynamicRecords( recordsAfter );
        channel.put( first( recordsAfter ).isCreated() ? (byte) 1 : 0 );
        return false;
    }

    @Override
    public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
    {
        channel.put( NeoCommandType.NEOSTORE_COMMAND ).putLong( command.getRecord().getNextProp() );
        return false;
    }

    @Override
    public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
    {
        channel.put( NeoCommandType.INDEX_ADD_COMMAND );
        writeToFile( command );
        return false;
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
    {
        channel.put( NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND );
        writeToFile( command );
        putIntOrLong( command.getStartNode() );
        putIntOrLong( command.getEndNode() );
        return false;
    }

    @Override
    public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
    {
        channel.put( NeoCommandType.INDEX_REMOVE_COMMAND );
        writeToFile( command );
        return false;
    }

    @Override
    public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
    {
        channel.put( NeoCommandType.INDEX_DELETE_COMMAND );
        writeIndexCommandHeader( command );
        return false;
    }

    @Override
    public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
    {
        channel.put( NeoCommandType.INDEX_CREATE_COMMAND );
        writeIndexCommandHeader( command );
        channel.putShort( (short) command.getConfig().size() );
        for ( Map.Entry<String,String> entry : command.getConfig().entrySet() )
        {
            write2bLengthAndString( channel, entry.getKey() );
            write2bLengthAndString( channel, entry.getValue() );
        }
        return false;
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        channel.put( NeoCommandType.INDEX_DEFINE_COMMAND );
        byte zero = 0;
        writeIndexCommandHeader( zero, zero, zero, zero, zero, zero, zero );
        writeMap( command.getIndexNameIdRange() );
        writeMap( command.getKeyIdRange() );
        return false;
    }

    @Override
    public boolean visitNodeCountsCommand( NodeCountsCommand command ) throws IOException
    {
        channel.put( NeoCommandType.UPDATE_NODE_COUNTS_COMMAND );
        channel.putInt( command.labelId() )
               .putLong( command.delta() );
        return false;
    }

    @Override
    public boolean visitRelationshipCountsCommand( RelationshipCountsCommand command ) throws IOException
    {
        channel.put( NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND );
        channel.putInt( command.startLabelId() )
               .putInt( command.typeId() )
               .putInt( command.endLabelId() )
               .putLong( command.delta() );
        return false;
    }

    private void writeMap( Map<String,Integer> map ) throws IOException
    {
        channel.put( (byte) map.size() );
        for ( Map.Entry<String,Integer> entry : map.entrySet() )
        {
            write2bLengthAndString( channel, entry.getKey() );
            int id = entry.getValue();
            channel.putShort( (short) id );
        }
    }

    public void writeToFile( IndexCommand command ) throws IOException
    {
        /* c: commandType
         * e: entityType
         * n: indexNameId
         * k: keyId
         * i: entityId
         * v: value type
         * u: value
         * x: 0=entityId needs 4b, 1=entityId needs 8b
         * y: 0=startNode needs 4b, 1=startNode needs 8b
         * z: 0=endNode needs 4b, 1=endNode needs 8b
         *
         * [cccv,vvex][yznn,nnnn][kkkk,kkkk]
         * [iiii,iiii] x 4 or 8
         * (either string value)
         * [llll,llll][llll,llll][llll,llll][string chars...]
         * (numeric value)
         * [uuuu,uuuu] x 2-8 (depending on value type)
         */
        writeIndexCommandHeader( command );
        putIntOrLong( command.getEntityId() );
        // Value
        Object value = command.getValue();
        switch ( command.getValueType() )
        {
        case IndexCommand.VALUE_TYPE_STRING:
            write3bLengthAndString( channel, value.toString() );
            break;
        case IndexCommand.VALUE_TYPE_SHORT:
            channel.putShort( ((Number) value).shortValue() );
            break;
        case IndexCommand.VALUE_TYPE_INT:
            channel.putInt( ((Number) value).intValue() );
            break;
        case IndexCommand.VALUE_TYPE_LONG:
            channel.putLong( ((Number) value).longValue() );
            break;
        case IndexCommand.VALUE_TYPE_FLOAT:
            channel.putFloat( ((Number) value).floatValue() );
            break;
        case IndexCommand.VALUE_TYPE_DOUBLE:
            channel.putDouble( ((Number) value).doubleValue() );
            break;
        case IndexCommand.VALUE_TYPE_NULL:
            break;
        default:
            throw new RuntimeException( "Unknown value type " + command.getValueType() );
        }
    }

    protected void writeIndexCommandHeader( IndexCommand command ) throws IOException
    {
        writeIndexCommandHeader( command.getValueType(), command.getEntityType(), needsLong( command.getEntityId() ),
                command.startNodeNeedsLong(), command.endNodeNeedsLong(), command.getIndexNameId(),
                command.getKeyId() );
    }

    protected void writeIndexCommandHeader( byte valueType, byte entityType, byte entityIdNeedsLong,
                                            byte startNodeNeedsLong, byte endNodeNeedsLong, int indexNameId,
                                            int keyId ) throws IOException
    {
        channel.put( (byte) ((valueType << 2) | (entityType << 1) | (entityIdNeedsLong)) );
        channel.put( (byte) ((startNodeNeedsLong << 7) | (endNodeNeedsLong << 6)) );
        channel.putShort( (short) indexNameId );
        channel.putShort( (short) keyId );
    }

    protected void putIntOrLong( long id ) throws IOException
    {
        if ( needsLong( id ) == 1 )
        {
            channel.putLong( id );
        }
        else
        {
            channel.putInt( (int) id );
        }
    }

    void writeDynamicRecords( Collection<DynamicRecord> records ) throws IOException
    {
        channel.putInt( records.size() ); // 4
        for ( DynamicRecord record : records )
        {
            writeDynamicRecord( record );
        }
    }

    void writeDynamicRecord( DynamicRecord record ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            if ( record.isStartRecord() )
            {
                inUse |= Record.FIRST_IN_CHAIN.byteValue();
            }
            channel.putLong( record.getId() ).putInt( record.getType() ).put( inUse ).putInt( record.getLength() )
                   .putLong( record.getNextBlock() );
            byte[] data = record.getData();
            assert data != null;
            channel.put( data, data.length );
        }
        else
        {
            byte inUse = Record.NOT_IN_USE.byteValue();
            channel.putLong( record.getId() ).putInt( record.getType() ).put( inUse );
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
        byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
        if ( record.getRelId() != -1 )
        {
            // Here we add 2, i.e. set the second lsb.
            inUse += Record.REL_PROPERTY.byteValue();
        }
        channel.put( inUse ); // 1
        channel.putLong( record.getNextProp() ).putLong( record.getPrevProp() ); // 8 + 8
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
        channel.put( (byte) record.numberOfProperties() ); // 1
        for ( PropertyBlock block : record )
        {
            assert block.getSize() > 0 : record + " seems kinda broken";
            writePropertyBlock( block );
        }
        writeDynamicRecords( record.getDeletedRecords() );
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
