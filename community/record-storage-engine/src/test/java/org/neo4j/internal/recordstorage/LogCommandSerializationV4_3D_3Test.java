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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LogCommandSerializationV4_3D_3Test extends LogCommandSerializationV4_2Test
{
    @Override
    protected CommandReader createReader()
    {
        return new LogCommandSerializationV4_3_D3();
    }

    @Override
    protected LogCommandSerialization writer()
    {
        return LogCommandSerializationV4_3_D3.INSTANCE;
    }

    @Test
    void shouldReadAndWriteMetaDataCommand() throws IOException
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        MetaDataRecord before = new MetaDataRecord( 12 );
        MetaDataRecord after = new MetaDataRecord( 12 );
        after.initialize( true, 999 );
        new Command.MetaDataCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.MetaDataCommand );

        Command.MetaDataCommand readCommand = (Command.MetaDataCommand) command;

        // Then
        assertBeforeAndAfterEquals( readCommand, before, after );
    }
}
