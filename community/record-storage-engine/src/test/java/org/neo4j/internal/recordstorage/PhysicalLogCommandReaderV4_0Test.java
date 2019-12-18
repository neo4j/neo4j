/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.recordstorage.PhysicalLogCommandReaderV4_0.markAfterRecordAsCreatedIfCommandLooksCreated;

class PhysicalLogCommandReaderV4_0Test
{
    @Test
    void shouldReadPropertyKeyCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord( 42 );
        PropertyKeyTokenRecord after = before.copy();
        after.initialize( true, 13 );
        after.setCreated();
        new Command.PropertyKeyTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyKeyTokenCommand );

        Command.PropertyKeyTokenCommand propertyKeyTokenCommand = (Command.PropertyKeyTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals( propertyKeyTokenCommand, before, after );
    }

    @Test
    void shouldReadInternalPropertyKeyCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord( 42 );
        PropertyKeyTokenRecord after = before.copy();
        after.initialize( true, 13 );
        after.setCreated();
        after.setInternal( true );
        new Command.PropertyKeyTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyKeyTokenCommand );

        Command.PropertyKeyTokenCommand propertyKeyTokenCommand = (Command.PropertyKeyTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals( propertyKeyTokenCommand, before, after );
    }

    @Test
    void shouldReadLabelCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        LabelTokenRecord before = new LabelTokenRecord( 42 );
        LabelTokenRecord after = before.copy();
        after.initialize( true, 13 );
        after.setCreated();
        new Command.LabelTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.LabelTokenCommand );

        Command.LabelTokenCommand labelTokenCommand = (Command.LabelTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals( labelTokenCommand, before, after );
    }

    @Test
    void shouldReadInternalLabelCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        LabelTokenRecord before = new LabelTokenRecord( 42 );
        LabelTokenRecord after = before.copy();
        after.initialize( true, 13 );
        after.setCreated();
        after.setInternal( true );
        new Command.LabelTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.LabelTokenCommand );

        Command.LabelTokenCommand labelTokenCommand = (Command.LabelTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals( labelTokenCommand, before, after );
    }

    @Test
    void shouldReadRelationshipTypeCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord( 42 );
        RelationshipTypeTokenRecord after = before.copy();
        after.initialize( true, 13 );
        after.setCreated();
        new Command.RelationshipTypeTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipTypeTokenCommand );

        Command.RelationshipTypeTokenCommand relationshipTypeTokenCommand = (Command.RelationshipTypeTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipTypeTokenCommand, before, after );
    }

    @Test
    void shouldReadInternalRelationshipTypeLabelCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord( 42 );
        RelationshipTypeTokenRecord after = before.copy();
        after.initialize( true, 13 );
        after.setCreated();
        after.setInternal( true );
        new Command.RelationshipTypeTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipTypeTokenCommand );

        Command.RelationshipTypeTokenCommand relationshipTypeTokenCommand = (Command.RelationshipTypeTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipTypeTokenCommand, before, after );
    }

    @Test
    void shouldReadRelationshipCommand() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord( 42, -1, -1, -1 );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        after.setCreated();
        new Command.RelationshipCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipCommand, before, after );
    }

    @Test
    void readRelationshipCommandWithSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        before.setSecondaryUnitIdOnLoad( 47 );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 8, 3, 4, 5, 6, 7, true, true );
        new Command.RelationshipCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals( relationshipCommand, before, after );
    }

    @Test
    void readRelationshipCommandWithNonRequiredSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        before.setSecondaryUnitIdOnLoad( 52 );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 8, 3, 4, 5, 6, 7, true, true );
        new Command.RelationshipCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals( relationshipCommand, before, after );
    }

    @Test
    void readRelationshipCommandWithFixedReferenceFormat() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        before.setUseFixedReferences( true );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 8, 3, 4, 5, 6, 7, true, true );
        after.setUseFixedReferences( true );
        new Command.RelationshipCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals( relationshipCommand, before, after );
        assertTrue( relationshipCommand.getBefore().isUseFixedReferences() );
        assertTrue( relationshipCommand.getAfter().isUseFixedReferences() );
    }

    @Test
    void shouldReadRelationshipGroupCommand() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 3 );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 3, 4, 5, 6, 7, 8, true );
        after.setCreated();

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
    }

    @Test
    void readRelationshipGroupCommandWithSecondaryUnit() throws IOException
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 3 );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 3, 4, 5, 6, 7, 8, true );
        after.setSecondaryUnitIdOnCreate( 17 );
        after.setCreated();

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
    }

    @Test
    void readRelationshipGroupCommandWithNonRequiredSecondaryUnit() throws IOException
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 3 );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 3, 4, 5, 6, 7, 8, true );
        after.setSecondaryUnitIdOnCreate( 17 );
        after.setCreated();

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
    }

    @Test
    void readRelationshipGroupCommandWithFixedReferenceFormat() throws IOException
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 3 );
        before.setUseFixedReferences( true );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 3, 4, 5, 6, 7, 8, true );
        after.setUseFixedReferences( true );
        after.setCreated();

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
        assertTrue( relationshipGroupCommand.getBefore().isUseFixedReferences() );
        assertTrue( relationshipGroupCommand.getAfter().isUseFixedReferences() );
    }

    @Test
    void nodeCommandWithFixedReferenceFormat() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        NodeRecord before = new NodeRecord( 42, true, false, 33, 99, 66 );
        NodeRecord after = new NodeRecord( 42, true, false, 33, 99, 66 );
        before.setUseFixedReferences( true );
        after.setUseFixedReferences( true );

        new Command.NodeCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.NodeCommand);

        Command.NodeCommand nodeCommand = (Command.NodeCommand) command;

        // Then
        assertBeforeAndAfterEquals( nodeCommand, before, after );
        assertTrue( nodeCommand.getBefore().isUseFixedReferences() );
        assertTrue( nodeCommand.getAfter().isUseFixedReferences() );
    }

    @Test
    void readPropertyCommandWithSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyRecord before = new PropertyRecord( 1 );
        PropertyRecord after = new PropertyRecord( 1 );
        after.setSecondaryUnitIdOnCreate( 78 );

        new Command.PropertyCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyCommand);

        Command.PropertyCommand propertyCommand = (Command.PropertyCommand) command;

        // Then
        assertBeforeAndAfterEquals( propertyCommand, before, after );
    }

    @Test
    void readPropertyCommandWithNonRequiredSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyRecord before = new PropertyRecord( 1 );
        PropertyRecord after = new PropertyRecord( 1 );
        after.setSecondaryUnitIdOnCreate( 78 );

        new Command.PropertyCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyCommand);

        Command.PropertyCommand propertyCommand = (Command.PropertyCommand) command;

        // Then
        assertBeforeAndAfterEquals( propertyCommand, before, after );
    }

    @Test
    void readPropertyCommandWithFixedReferenceFormat() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyRecord before = new PropertyRecord( 1 );
        PropertyRecord after = new PropertyRecord( 1 );
        before.setUseFixedReferences( true );
        after.setUseFixedReferences( true );

        new Command.PropertyCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyCommand);

        Command.PropertyCommand propertyCommand = (Command.PropertyCommand) command;

        // Then
        assertBeforeAndAfterEquals( propertyCommand, before, after );
        assertTrue( propertyCommand.getBefore().isUseFixedReferences() );
        assertTrue( propertyCommand.getAfter().isUseFixedReferences() );
    }

    @Test
    void shouldReadSomeCommands() throws Exception
    {
        // GIVEN
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        Commands.createNode( 0 ).serialize( channel );
        Commands.createNode( 1 ).serialize( channel );
        Commands.createRelationshipTypeToken( 0, 0 ).serialize( channel );
        Commands.createRelationship( 0, 0, 1, 0 ).serialize( channel );
        Commands.createPropertyKeyToken( 0, 0 ).serialize( channel );
        Commands.createProperty( 0, PropertyType.SHORT_STRING, 0 ).serialize( channel );
        CommandReader reader = createReader();

        // THEN
        assertTrue( reader.read( channel ) instanceof Command.NodeCommand );
        assertTrue( reader.read( channel ) instanceof Command.NodeCommand );
        assertTrue( reader.read( channel ) instanceof Command.RelationshipTypeTokenCommand );
        assertTrue( reader.read( channel ) instanceof Command.RelationshipCommand );
        assertTrue( reader.read( channel ) instanceof Command.PropertyKeyTokenCommand );
        assertTrue( reader.read( channel ) instanceof Command.PropertyCommand );
    }

    @RepeatedTest( 100 )
    void shouldReadSchemaCommand() throws Exception
    {
        // given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        SchemaRecord before = createRandomSchemaRecord();
        SchemaRecord after = createRandomSchemaRecord();
        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );

        long id = after.getId();
        SchemaRule rule = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2, 3 ) ).withName( "index_" + id ).materialise( id );
        new Command.SchemaRuleCommand( before, after, rule ).serialize( channel );

        CommandReader reader = createReader();
        Command.SchemaRuleCommand command = (Command.SchemaRuleCommand) reader.read( channel );

        assertBeforeAndAfterEquals( command, before, after );
    }

    private SchemaRecord createRandomSchemaRecord()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        SchemaRecord record = new SchemaRecord( 42 );
        boolean inUse = rng.nextBoolean();
        if ( inUse )
        {
            record.initialize( inUse, rng.nextLong() );
            if ( rng.nextBoolean() )
            {
                record.setCreated();
            }
            record.setConstraint( rng.nextBoolean() );
            record.setUseFixedReferences( rng.nextBoolean() );
            boolean requiresSecondaryUnit = rng.nextBoolean();
            if ( requiresSecondaryUnit )
            {
                record.setSecondaryUnitIdOnLoad( rng.nextLong() );
            }
        }
        else
        {
            record.clear();
        }
        return record;
    }

    private BaseCommandReader createReader()
    {
        return new PhysicalLogCommandReaderV4_0();
    }

    private static <RECORD extends AbstractBaseRecord> void assertBeforeAndAfterEquals( Command.BaseCommand<RECORD> command, RECORD before, RECORD after )
    {
        assertEqualsIncludingFlags( before, command.getBefore() );
        assertEqualsIncludingFlags( after, command.getAfter() );
    }

    private static <RECORD extends AbstractBaseRecord> void assertEqualsIncludingFlags( RECORD expected, RECORD record )
    {
        assertEquals( expected, record );
        assertEquals( expected.isCreated(), record.isCreated() );
        assertEquals( expected.isUseFixedReferences(), record.isUseFixedReferences() );
        assertEquals( expected.isSecondaryUnitCreated(), record.isSecondaryUnitCreated() );
    }
}
