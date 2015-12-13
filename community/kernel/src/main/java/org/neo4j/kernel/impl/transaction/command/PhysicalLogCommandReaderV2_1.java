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

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.COLLECTION_DYNAMIC_RECORD_ADDER;
import static org.neo4j.kernel.impl.util.Bits.bitFlag;
import static org.neo4j.kernel.impl.util.Bits.notFlag;

public class PhysicalLogCommandReaderV2_1 extends PhysicalLogCommandReaderV2_0
{
    @Override
    protected Command read( byte commandType, ReadableLogChannel channel ) throws IOException
    {
        switch ( commandType )
        {
        case NeoCommandType.REL_GROUP_COMMAND:
            return readRelationshipGroupCommand( channel );
        default:
            return super.read( commandType, channel );
        }
    }

    /**
     * CHANGE: added dense field
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
        if ( inUse )
        {
            boolean dense = channel.get() == 1;
            record = new NodeRecord( id, dense, channel.getLong(), channel.getLong() );
            // labels
            long labelField = channel.getLong();
            Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<>();
            readDynamicRecords( channel, dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
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

    /**
     * CHANGE: added first in source/target node chain
     *         added CREATED_IN_TX bit in inUse byte
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
            record = new RelationshipRecord( id, -1, -1, -1 );
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

    protected Command readRelationshipGroupCommand( ReadableLogChannel channel ) throws IOException
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

    /**
     * CHANGE: returns null on MalformedSchemaRuleException
     */
    @Override
    protected SchemaRule readSchemaRule( Collection<DynamicRecord> recordsBefore )
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
}
