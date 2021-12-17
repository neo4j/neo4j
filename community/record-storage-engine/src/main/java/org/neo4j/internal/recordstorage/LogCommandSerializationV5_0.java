/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import java.io.IOException;

import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static org.neo4j.internal.recordstorage.CommandReading.PROPERTY_INDEX_DYNAMIC_RECORD_ADDER;
import static org.neo4j.util.Bits.bitFlag;
import static org.neo4j.util.Bits.bitFlags;

class LogCommandSerializationV5_0 extends LogCommandSerializationV4_4
{
    static final LogCommandSerializationV5_0 INSTANCE = new LogCommandSerializationV5_0();

    @Override
    KernelVersion version()
    {
        return KernelVersion.V5_0;
    }

    @Override
    protected Command readSchemaRuleCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        byte schemaRulePresence = channel.get();
        boolean hasSchemaRule = schemaRulePresence == SchemaRecord.COMMAND_HAS_SCHEMA_RULE;
        SchemaRecord before = readSchemaRecord( id, channel );
        SchemaRecord after = readSchemaRecord( id, channel );
        SchemaRule schemaRule = null;
        if ( hasSchemaRule )
        {
            schemaRule = readSchemaRule( id, channel );
        }
        return new Command.SchemaRuleCommand( this, before, after, schemaRule );
    }

    private static SchemaRecord readSchemaRecord( long id, ReadableChannel channel ) throws IOException
    {
        SchemaRecord schemaRecord = new SchemaRecord( id );
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean createdInTx = bitFlag( flags, Record.CREATED_IN_TX );
        schemaRecord.setInUse( inUse );
        if ( inUse )
        {
            byte schemaFlags = channel.get();
            schemaRecord.setConstraint( bitFlag( schemaFlags, SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT ) );
            schemaRecord.setNextProp( channel.getLong() );
        }
        if ( createdInTx )
        {
            schemaRecord.setCreated();
        }

        return schemaRecord;
    }

    @Override
    public void writeSchemaRuleCommand( WritableChannel channel, Command.SchemaRuleCommand command ) throws IOException
    {
        channel.put( NeoCommandType.SCHEMA_RULE_COMMAND );
        channel.putLong( command.getBefore().getId() );
        SchemaRule schemaRule = command.getSchemaRule();
        boolean hasSchemaRule = schemaRule != null;
        channel.put( hasSchemaRule ? SchemaRecord.COMMAND_HAS_SCHEMA_RULE : SchemaRecord.COMMAND_HAS_NO_SCHEMA_RULE );
        writeSchemaRecord( channel, command.getBefore() );
        writeSchemaRecord( channel, command.getAfter() );
        if ( hasSchemaRule )
        {
            writeSchemaRule( channel, schemaRule );
        }
    }

    private static void writeSchemaRecord( WritableChannel channel, SchemaRecord record ) throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                               bitFlag( record.isCreated(), Record.CREATED_IN_TX ) );
        channel.put( flags );
        if ( record.inUse() )
        {
            byte schemaFlags = bitFlag( record.isConstraint(), SchemaRecord.SCHEMA_FLAG_IS_CONSTRAINT );
            channel.put( schemaFlags );
            channel.putLong( record.getNextProp() );
        }
    }
}
