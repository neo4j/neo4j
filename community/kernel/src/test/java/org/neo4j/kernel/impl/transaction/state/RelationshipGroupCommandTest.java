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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class RelationshipGroupCommandTest
{
    @Test
    public void shouldSerializeAndDeserializeUnusedRecords() throws Exception
    {
        // Given
        RelationshipGroupRecord before = new RelationshipGroupRecord( 10, 12, 13, 14, 15, 16, 17, true );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 10, 12 );
        after.setInUse( false );

        // When
        assertSerializationWorksFor( new Command.RelationshipGroupCommand( before, after ) );
    }

    @Test
    public void shouldSerializeCreatedRecord() throws Exception
    {
        // Given
        RelationshipGroupRecord before = new RelationshipGroupRecord( 10, 12 );
        before.setInUse( false );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 10, 12, 13, 14, 15, 16, 17, true );
        after.setCreated();

        // When
        assertSerializationWorksFor( new Command.RelationshipGroupCommand( before, after ) );
    }

    private void assertSerializationWorksFor( Command.RelationshipGroupCommand cmd ) throws IOException
    {
        InMemoryLogChannel channel = new InMemoryLogChannel();
        CommandWriter commandWriter = new CommandWriter( channel );
        commandWriter.visitRelationshipGroupCommand( cmd );

        CommandReader commandReader = new PhysicalLogCommandReaderV2_2();
        Command.RelationshipGroupCommand result = (Command.RelationshipGroupCommand) commandReader.read( channel );

        RelationshipGroupRecord recordBefore = cmd.getBefore();
        RelationshipGroupRecord recordAfter = result.getAfter();

        // Then
        assertThat( recordBefore.getFirstIn(), equalTo( recordAfter.getFirstIn() ) );
        assertThat( recordBefore.getFirstOut(), equalTo( recordAfter.getFirstOut() ) );
        assertThat( recordBefore.getFirstLoop(), equalTo( recordAfter.getFirstLoop() ) );
        assertThat( recordBefore.getNext(), equalTo( recordAfter.getNext() ) );
        assertThat( recordBefore.getOwningNode(), equalTo( recordAfter.getOwningNode() ) );
        assertThat( recordBefore.getPrev(), equalTo( recordAfter.getPrev() ) );
        assertThat( recordBefore.getType(), equalTo( recordAfter.getType() ) );
    }
}
