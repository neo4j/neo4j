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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.RandomSupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class LogCommandSerializationV4_3D_3Test extends LogCommandSerializationV4_2Test
{
    @Inject
    private RandomSupport random;

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

    @Test
    void shouldReadRelationshipGroupCommandIncludingExternalDegrees() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42 ).initialize( false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42 ).initialize( true, 3, 4, 5, 6, 7, 8 );
        after.setHasExternalDegreesOut( random.nextBoolean() );
        after.setHasExternalDegreesIn( random.nextBoolean() );
        after.setHasExternalDegreesLoop( random.nextBoolean() );
        after.setCreated();

        new Command.RelationshipGroupCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
    }

    @Test
    void shouldReadRelationshipGroupExtendedCommandIncludingExternalDegrees() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42 ).initialize( false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42 ).initialize( true, (1 << Short.SIZE) + 10, 4, 5, 6, 7, 8 );
        after.setHasExternalDegreesOut( random.nextBoolean() );
        after.setHasExternalDegreesIn( random.nextBoolean() );
        after.setHasExternalDegreesLoop( random.nextBoolean() );
        after.setCreated();

        new Command.RelationshipGroupCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
    }

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
}
