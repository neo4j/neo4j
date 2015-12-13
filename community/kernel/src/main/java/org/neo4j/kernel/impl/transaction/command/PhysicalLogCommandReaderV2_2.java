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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.COLLECTION_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.util.Bits.bitFlag;
import static org.neo4j.kernel.impl.util.Bits.notFlag;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read2bLengthAndString;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read2bMap;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read3bLengthAndString;

public class PhysicalLogCommandReaderV2_2 extends PhysicalLogCommandReaderV2_1
{
    @Override
    protected Command read( byte commandType, ReadableLogChannel channel ) throws IOException
    {
        switch ( commandType )
        {
        case NeoCommandType.INDEX_DEFINE_COMMAND:
            return readIndexDefineCommand( channel );
        case NeoCommandType.INDEX_ADD_COMMAND:
            return readIndexAddNodeCommand( channel );
        case NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND:
            return readIndexAddRelationshipCommand( channel );
        case NeoCommandType.INDEX_REMOVE_COMMAND:
            return readIndexRemoveCommand( channel );
        case NeoCommandType.INDEX_DELETE_COMMAND:
            return readIndexDeleteCommand( channel );
        case NeoCommandType.INDEX_CREATE_COMMAND:
            return readIndexCreateCommand( channel );
        case NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND:
            return readRelationshipCountsCommand( channel );
        case NeoCommandType.UPDATE_NODE_COUNTS_COMMAND:
            return readNodeCountsCommand( channel );
        default:
            return super.read( commandType, channel );
        }
    }

    /**
     * CHANGE: for some reason we always write number of dynamic records, even if !inUse
     */
    @Override
    protected NodeRecord readNodeRecord( ReadableLogChannel channel, long id ) throws IOException
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

    /**
     * CHANGE: store type for deleted relationships
     */
    @Override
    protected Command readRelationshipCommand( ReadableLogChannel channel ) throws IOException
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

    /**
     * CHANGE: removed unused txId (8B)
     */
    @Override
    protected Command readSchemaRuleCommand( ReadableLogChannel channel ) throws IOException
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

        SchemaRule rule = first( recordsAfter ).inUse() ?
                          readSchemaRule( recordsAfter ) :
                          readSchemaRule( recordsBefore );

        Command.SchemaRuleCommand command = new Command.SchemaRuleCommand();
        command.init( recordsBefore, recordsAfter, rule );
        return command;
    }

    protected static class IndexCommandHeader
    {
        protected byte valueType;
        protected byte entityType;
        protected boolean entityIdNeedsLong;
        protected int indexNameId;
        protected boolean startNodeNeedsLong;
        protected boolean endNodeNeedsLong;
        protected int keyId;

        protected IndexCommandHeader( byte valueType, byte entityType, boolean entityIdNeedsLong,
                int indexNameId, boolean startNodeNeedsLong, boolean endNodeNeedsLong, int keyId )
        {
            this.valueType = valueType;
            this.entityType = entityType;
            this.entityIdNeedsLong = entityIdNeedsLong;
            this.indexNameId = indexNameId;
            this.startNodeNeedsLong = startNodeNeedsLong;
            this.endNodeNeedsLong = endNodeNeedsLong;
            this.keyId = keyId;
        }
    }

    protected Command readIndexAddNodeCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        AddNodeCommand command = new AddNodeCommand();
        command.init( header.indexNameId, entityId.longValue(), header.keyId, value );
        return command;
    }

    protected Command readIndexAddRelationshipCommand( ReadableLogChannel channel ) throws IOException
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

    protected Command readIndexRemoveCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        RemoveCommand command = new RemoveCommand();
        command.init( header.indexNameId, header.entityType, entityId.longValue(), header.keyId, value );
        return command;
    }

    protected Command readIndexDeleteCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        DeleteCommand command = new DeleteCommand();
        command.init( header.indexNameId, header.entityType );
        return command;
    }

    protected Command readIndexCreateCommand( ReadableLogChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Map<String,String> config = read2bMap( channel );
        CreateCommand command = new CreateCommand();
        command.init( header.indexNameId, header.entityType, config );
        return command;
    }

    protected Command readIndexDefineCommand( ReadableLogChannel channel ) throws IOException
    {
        readIndexCommandHeader( channel );
        Map<String,Integer> indexNames = readMap( channel );
        Map<String,Integer> keys = readMap( channel );
        IndexDefineCommand command = new IndexDefineCommand();
        command.init( indexNames, keys );
        return command;
    }

    protected Command readNodeCountsCommand( ReadableLogChannel channel ) throws IOException
    {
        int labelId = channel.getInt();
        long delta = channel.getLong();
        Command.NodeCountsCommand command = new Command.NodeCountsCommand();
        command.init( labelId, delta );
        return command;
    }

    protected Command readRelationshipCountsCommand( ReadableLogChannel channel ) throws IOException
    {
        int startLabelId = channel.getInt();
        int typeId = channel.getInt();
        int endLabelId = channel.getInt();
        long delta = channel.getLong();
        Command.RelationshipCountsCommand command = new Command.RelationshipCountsCommand();
        command.init( startLabelId, typeId, endLabelId, delta );
        return command;
    }

    protected Map<String,Integer> readMap( ReadableLogChannel channel ) throws IOException
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

    protected IndexCommandHeader readIndexCommandHeader( ReadableLogChannel channel ) throws IOException
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
        return new IndexCommandHeader( valueType, entityType, entityIdNeedsLong,
                indexNameId, startNodeNeedsLong, endNodeNeedsLong, keyId );
    }

    protected Object readIndexValue( byte valueType, ReadableLogChannel channel ) throws IOException
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
