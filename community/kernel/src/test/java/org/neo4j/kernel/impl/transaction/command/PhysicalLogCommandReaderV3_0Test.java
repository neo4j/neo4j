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

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PhysicalLogCommandReaderV3_0Test
{
    @Test
    public void shouldReadRelationshipCommand() throws Throwable
    {
        // Given
        InMemoryLogChannel channel = new InMemoryLogChannel();
        CommandWriter writer = new CommandWriter( channel );
        RelationshipRecord before = new RelationshipRecord( 42, -1, -1, -1 );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        writer.visitRelationshipCommand( new Command.RelationshipCommand( before, after ) );

        // When
        PhysicalLogCommandReaderV3_0 reader = new PhysicalLogCommandReaderV3_0();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;

        // Then
        assertEquals( before, relationshipCommand.getBefore() );
        assertEquals( after, relationshipCommand.getAfter() );
    }

    @Test
    public void shouldReadRelationshipGroupCommand() throws Throwable
    {
        // Given
        InMemoryLogChannel channel = new InMemoryLogChannel();
        CommandWriter writer = new CommandWriter( channel );
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 3 );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 3, 4, 5, 6, 7, 8, true );
        after.setCreated();

        writer.visitRelationshipGroupCommand( new Command.RelationshipGroupCommand( before, after ) );

        // When
        PhysicalLogCommandReaderV3_0 reader = new PhysicalLogCommandReaderV3_0();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertEquals( before, relationshipGroupCommand.getBefore() );
        assertEquals( after, relationshipGroupCommand.getAfter() );
    }

    @Test
    public void shouldReadNeoStoreCommand() throws Throwable
    {
        // Given
        InMemoryLogChannel channel = new InMemoryLogChannel();
        CommandWriter writer = new CommandWriter( channel );
        NeoStoreRecord before = new NeoStoreRecord();
        NeoStoreRecord after = new NeoStoreRecord();
        after.setNextProp( 42 );

        writer.visitNeoStoreCommand( new Command.NeoStoreCommand( before, after ) );

        // When
        PhysicalLogCommandReaderV3_0 reader = new PhysicalLogCommandReaderV3_0();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.NeoStoreCommand);

        Command.NeoStoreCommand neoStoreCommand = (Command.NeoStoreCommand) command;

        // Then
        assertEquals( before.getNextProp(), neoStoreCommand.getBefore().getNextProp() );
        assertEquals( after.getNextProp(), neoStoreCommand.getAfter().getNextProp() );
    }
}
