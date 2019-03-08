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

import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PhysicalLogCommandReaderV4_0Test
{
    @Test
    void shouldReadPropertyKeyCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord( 42 );
        PropertyKeyTokenRecord after = before.clone();
        after.initialize( true, 13 );
        after.setCreated();
        new Command.PropertyKeyTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyKeyTokenCommand );

        Command.PropertyKeyTokenCommand propertyKeyTokenCommand = (Command.PropertyKeyTokenCommand) command;

        // Then
        assertEquals( before, propertyKeyTokenCommand.getBefore() );
        assertEquals( after, propertyKeyTokenCommand.getAfter() );
    }

    @Test
    void shouldReadInternalPropertyKeyCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord( 42 );
        PropertyKeyTokenRecord after = before.clone();
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
        assertEquals( before, propertyKeyTokenCommand.getBefore() );
        assertEquals( after, propertyKeyTokenCommand.getAfter() );
    }

    @Test
    void shouldReadLabelCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        LabelTokenRecord before = new LabelTokenRecord( 42 );
        LabelTokenRecord after = before.clone();
        after.initialize( true, 13 );
        after.setCreated();
        new Command.LabelTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.LabelTokenCommand );

        Command.LabelTokenCommand labelTokenCommand = (Command.LabelTokenCommand) command;

        // Then
        assertEquals( before, labelTokenCommand.getBefore() );
        assertEquals( after, labelTokenCommand.getAfter() );
    }

    @Test
    void shouldReadInternalLabelCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        LabelTokenRecord before = new LabelTokenRecord( 42 );
        LabelTokenRecord after = before.clone();
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
        assertEquals( before, labelTokenCommand.getBefore() );
        assertEquals( after, labelTokenCommand.getAfter() );
    }

    @Test
    void shouldReadRelationshipTypeCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord( 42 );
        RelationshipTypeTokenRecord after = before.clone();
        after.initialize( true, 13 );
        after.setCreated();
        new Command.RelationshipTypeTokenCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipTypeTokenCommand );

        Command.RelationshipTypeTokenCommand relationshipTypeTokenCommand = (Command.RelationshipTypeTokenCommand) command;

        // Then
        assertEquals( before, relationshipTypeTokenCommand.getBefore() );
        assertEquals( after, relationshipTypeTokenCommand.getAfter() );
    }

    @Test
    void shouldReadInternalRelationshipTypeLabelCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord( 42 );
        RelationshipTypeTokenRecord after = before.clone();
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
        assertEquals( before, relationshipTypeTokenCommand.getBefore() );
        assertEquals( after, relationshipTypeTokenCommand.getAfter() );
    }

    @Test
    void shouldReadRelationshipCommand() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord( 42, -1, -1, -1 );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        new Command.RelationshipCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;

        // Then
        assertEquals( before, relationshipCommand.getBefore() );
        assertEquals( after, relationshipCommand.getAfter() );
    }

    @Test
    void readRelationshipCommandWithSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        before.setRequiresSecondaryUnit( true );
        before.setSecondaryUnitId( 47 );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 8, 3, 4, 5, 6, 7, true, true );
        new Command.RelationshipCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertEquals( before, relationshipCommand.getBefore() );
        verifySecondaryUnit( before, relationshipCommand.getBefore() );
        assertEquals( after, relationshipCommand.getAfter() );
    }

    @Test
    void readRelationshipCommandWithNonRequiredSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true );
        before.setRequiresSecondaryUnit( false );
        before.setSecondaryUnitId( 52 );
        RelationshipRecord after = new RelationshipRecord( 42, true, 1, 8, 3, 4, 5, 6, 7, true, true );
        new Command.RelationshipCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertEquals( before, relationshipCommand.getBefore() );
        verifySecondaryUnit( before, relationshipCommand.getBefore() );
        assertEquals( after, relationshipCommand.getAfter() );
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
        assertEquals( before, relationshipCommand.getBefore() );
        assertTrue( relationshipCommand.getBefore().isUseFixedReferences() );
        assertEquals( after, relationshipCommand.getAfter() );
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
        assertEquals( before, relationshipGroupCommand.getBefore() );
        assertEquals( after, relationshipGroupCommand.getAfter() );
    }

    @Test
    void readRelationshipGroupCommandWithSecondaryUnit() throws IOException
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 3 );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 3, 4, 5, 6, 7, 8, true );
        after.setRequiresSecondaryUnit( true );
        after.setSecondaryUnitId( 17 );
        after.setCreated();

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertEquals( before, relationshipGroupCommand.getBefore() );
        assertEquals( after, relationshipGroupCommand.getAfter() );
        verifySecondaryUnit( after, relationshipGroupCommand.getAfter() );
    }

    @Test
    void readRelationshipGroupCommandWithNonRequiredSecondaryUnit() throws IOException
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42, 3 );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42, 3, 4, 5, 6, 7, 8, true );
        after.setRequiresSecondaryUnit( false );
        after.setSecondaryUnitId( 17 );
        after.setCreated();

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertEquals( before, relationshipGroupCommand.getBefore() );
        assertEquals( after, relationshipGroupCommand.getAfter() );
        verifySecondaryUnit( after, relationshipGroupCommand.getAfter() );
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

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertEquals( before, relationshipGroupCommand.getBefore() );
        assertEquals( after, relationshipGroupCommand.getAfter() );
        assertTrue( relationshipGroupCommand.getBefore().isUseFixedReferences() );
        assertTrue( relationshipGroupCommand.getAfter().isUseFixedReferences() );
    }

    @Test
    void shouldReadNeoStoreCommand() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        NeoStoreRecord before = new NeoStoreRecord();
        NeoStoreRecord after = new NeoStoreRecord();
        after.setNextProp( 42 );

        new Command.NeoStoreCommand( before, after ).serialize( channel );

        // When
        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.NeoStoreCommand);

        Command.NeoStoreCommand neoStoreCommand = (Command.NeoStoreCommand) command;

        // Then
        assertEquals( before.getNextProp(), neoStoreCommand.getBefore().getNextProp() );
        assertEquals( after.getNextProp(), neoStoreCommand.getAfter().getNextProp() );
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
        assertEquals( before, nodeCommand.getBefore() );
        assertEquals( after, nodeCommand.getAfter() );
        assertTrue( nodeCommand.getBefore().isUseFixedReferences() );
        assertTrue( nodeCommand.getAfter().isUseFixedReferences() );
    }

    @Test
    void readPropertyCommandWithSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyRecord before = new PropertyRecord( 1 );
        PropertyRecord after = new PropertyRecord( 2 );
        after.setRequiresSecondaryUnit( true );
        after.setSecondaryUnitId( 78 );

        new Command.PropertyCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyCommand);

        Command.PropertyCommand neoStoreCommand = (Command.PropertyCommand) command;

        // Then
        assertEquals( before.getNextProp(), neoStoreCommand.getBefore().getNextProp() );
        assertEquals( after.getNextProp(), neoStoreCommand.getAfter().getNextProp() );
        verifySecondaryUnit( after, neoStoreCommand.getAfter() );
    }

    @Test
    void readPropertyCommandWithNonRequiredSecondaryUnit() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyRecord before = new PropertyRecord( 1 );
        PropertyRecord after = new PropertyRecord( 2 );
        after.setRequiresSecondaryUnit( false );
        after.setSecondaryUnitId( 78 );

        new Command.PropertyCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyCommand);

        Command.PropertyCommand neoStoreCommand = (Command.PropertyCommand) command;

        // Then
        assertEquals( before.getNextProp(), neoStoreCommand.getBefore().getNextProp() );
        assertEquals( after.getNextProp(), neoStoreCommand.getAfter().getNextProp() );
        verifySecondaryUnit( after, neoStoreCommand.getAfter() );
    }

    @Test
    void readPropertyCommandWithFixedReferenceFormat() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyRecord before = new PropertyRecord( 1 );
        PropertyRecord after = new PropertyRecord( 2 );
        before.setUseFixedReferences( true );
        after.setUseFixedReferences( true );

        new Command.PropertyCommand( before, after ).serialize( channel );

        BaseCommandReader reader = createReader();
        Command command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyCommand);

        Command.PropertyCommand neoStoreCommand = (Command.PropertyCommand) command;

        // Then
        assertEquals( before.getNextProp(), neoStoreCommand.getBefore().getNextProp() );
        assertEquals( after.getNextProp(), neoStoreCommand.getAfter().getNextProp() );
        assertTrue( neoStoreCommand.getBefore().isUseFixedReferences() );
        assertTrue( neoStoreCommand.getAfter().isUseFixedReferences() );
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
        if ( !before.inUse() && after.inUse() )
        {
            after.setCreated();
        }

        SchemaRule rule = new DefaultStorageIndexReference( SchemaDescriptorFactory.forLabel( 1, 2, 3 ), false, after.getId(), null );
        new Command.SchemaRuleCommand( before, after, rule ).serialize( channel );

        CommandReader reader = createReader();
        Command.SchemaRuleCommand command = (Command.SchemaRuleCommand) reader.read( channel );

        String commandString = command.toString();
        assertSchemaRecordEquals( commandString + " (before) ", before, command.getBefore() );
        assertSchemaRecordEquals( commandString + " ( after) ", after, command.getAfter() );
    }

    private void assertSchemaRecordEquals( String commandString, SchemaRecord expectedRecord, SchemaRecord actualRecord )
    {
        assertThat( commandString + ".getId", actualRecord.getId(), is( expectedRecord.getId() ) );
        assertThat( commandString + ".inUse", actualRecord.inUse(), is( expectedRecord.inUse() ) );
        assertThat( commandString + ".isCreated", actualRecord.isCreated(), is( expectedRecord.isCreated() ) );
        assertThat( commandString + ".isUseFixedReferences", actualRecord.isUseFixedReferences(), is( expectedRecord.isUseFixedReferences() ) );
        assertThat( commandString + ".hasSecondaryUnitId", actualRecord.hasSecondaryUnitId(), is( expectedRecord.hasSecondaryUnitId() ) );
        assertThat( commandString + ".getSecondaryUnitId", actualRecord.getSecondaryUnitId(), is( expectedRecord.getSecondaryUnitId() ) );
        assertThat( commandString + ".isConstraint", actualRecord.isConstraint(), is( expectedRecord.isConstraint() ) );
        assertThat( commandString + ".getNextProp", actualRecord.getNextProp(), is( expectedRecord.getNextProp() ) );
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
                record.setRequiresSecondaryUnit( rng.nextBoolean() );
                record.setSecondaryUnitId( rng.nextLong() );
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

    private <T extends AbstractBaseRecord> void verifySecondaryUnit( T record, T commandRecord )
    {
        assertEquals( record.requiresSecondaryUnit(),
                commandRecord.requiresSecondaryUnit(), "Secondary unit requirements should be the same" );
        assertEquals( record.getSecondaryUnitId(),
                commandRecord.getSecondaryUnitId(), "Secondary unit ids should be the same" );
    }
}
