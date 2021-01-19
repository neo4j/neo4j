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

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;

class LogCommandSerializationV4_3_D3 extends LogCommandSerializationV4_2
{
    static final LogCommandSerializationV4_3_D3 INSTANCE = new LogCommandSerializationV4_3_D3();

    @Override
    protected Command readMetaDataCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        MetaDataRecord before = readMetaDataRecord( id, channel );
        MetaDataRecord after = readMetaDataRecord( id, channel );
        return new Command.MetaDataCommand( this, before, after );
    }

    private MetaDataRecord readMetaDataRecord( long id, ReadableChannel channel ) throws IOException
    {
        long value = channel.getLong();
        MetaDataRecord record = new MetaDataRecord();
        record.setId( id );
        record.initialize( true, value );
        return record;
    }

    @Override
    public void writeMetaDataCommand( WritableChannel channel, Command.MetaDataCommand command ) throws IOException
    {
        channel.put( NeoCommandType.META_DATA_COMMAND );
        channel.putLong( command.getKey() );
        writeMetaDataRecord( channel, command.getBefore() );
        writeMetaDataRecord( channel, command.getAfter() );
    }

    private void writeMetaDataRecord( WritableChannel channel, MetaDataRecord record ) throws IOException
    {
        channel.putLong( record.getValue() );
    }
}
