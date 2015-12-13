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
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.transaction.command.CommandReaderFactory.COLLECTION_DYNAMIC_RECORD_ADDER;

public class PhysicalLogCommandReaderV2_0 extends PhysicalLogCommandReaderV1_9
{
    @Override
    protected Command read( byte commandType, ReadableLogChannel channel ) throws IOException
    {
        switch ( commandType )
        {
        case NeoCommandType.LABEL_KEY_COMMAND:
            return readLabelTokenCommand( channel );
        case NeoCommandType.SCHEMA_RULE_COMMAND:
            return readSchemaRuleCommand( channel );
        default:
            return super.read( commandType, channel );
        }
    }

    /**
     * CHANGE: before/after records
     */
    @Override
    protected Command readNodeCommand( ReadableLogChannel channel ) throws IOException
    {
        long id = channel.getLong();

        NodeRecord before = readNodeRecord( channel, id );
        if ( before == null )
        {
            return null;
        }

        NodeRecord after = readNodeRecord( channel, id );
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

    /**
     * CHANGE: before/after records
     */
    @Override
    protected Command readPropertyCommand( ReadableLogChannel channel ) throws IOException
    {
        // ID
        long id = channel.getLong(); // 8

        // BEFORE
        PropertyRecord before = readPropertyRecord( channel, id );
        if ( before == null )
        {
            return null;
        }

        // AFTER
        PropertyRecord after = readPropertyRecord( channel, id );
        if ( after == null )
        {
            return null;
        }

        Command.PropertyCommand command = new Command.PropertyCommand();
        command.init( before, after );
        return command;
    }

    protected Command readLabelTokenCommand( ReadableLogChannel channel ) throws IOException
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

        // read and ignore transaction id which is not used anymore
        channel.getLong();

        SchemaRule rule = first( recordsAfter ).inUse() ?
                          readSchemaRule( recordsAfter ) :
                          readSchemaRule( recordsBefore );

        Command.SchemaRuleCommand command = new Command.SchemaRuleCommand();
        command.init( recordsBefore, recordsAfter, rule );
        return command;
    }

    /**
     * CHANGE: added labels field
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
            record = new NodeRecord( id, false, channel.getLong(), channel.getLong() );
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

    protected SchemaRule readSchemaRule( Collection<DynamicRecord> recordsBefore )
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
