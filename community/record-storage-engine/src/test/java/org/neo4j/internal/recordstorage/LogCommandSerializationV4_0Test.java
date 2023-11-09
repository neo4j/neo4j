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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.id.BatchingIdSequence;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StandardDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.recordstorage.LogCommandSerializationV4_0.markAfterRecordAsCreatedIfCommandLooksCreated;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

@ExtendWith( RandomExtension.class )
class LogCommandSerializationV4_0Test
{
    static final long NULL_REF = NULL_REFERENCE.longValue();

    @Inject
    private RandomSupport random;

    @Test
    void shouldReadPropertyKeyCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord( 42 );
        PropertyKeyTokenRecord after = before.copy();
        after.initialize( true, 13 );
        after.setCreated();
        new Command.PropertyKeyTokenCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
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
        new Command.PropertyKeyTokenCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
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
        new Command.LabelTokenCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
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
        new Command.LabelTokenCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
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
        new Command.RelationshipTypeTokenCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
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
        new Command.RelationshipTypeTokenCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipTypeTokenCommand );

        Command.RelationshipTypeTokenCommand relationshipTypeTokenCommand = (Command.RelationshipTypeTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipTypeTokenCommand, before, after );
    }

    @RepeatedTest( 200 )
    void shouldReadRelationshipCommand() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        long id = randomEntityId();
        RelationshipRecord before = new RelationshipRecord( id );
        RelationshipRecord after = new RelationshipRecord( id );
        randomizeBeforeAfterRecords( before, after, this::randomizeRelationshipRecord, true );

        new Command.RelationshipCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipCommand, before, after );
    }

    @RepeatedTest( 200 )
    void shouldReadRelationshipGroupCommand() throws Throwable
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        long id = randomEntityId();
        RelationshipGroupRecord before = new RelationshipGroupRecord( id );
        RelationshipGroupRecord after = new RelationshipGroupRecord( id );
        randomizeBeforeAfterRecords( before, after, this::randomizeRelationshipGroupRecord, true );

        new Command.RelationshipGroupCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand );

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
    }

    @Test
    public void readRelationshipGroupWithBiggerThanShortRelationshipType() throws IOException
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before = new RelationshipGroupRecord( 42 ).initialize( false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF );
        RelationshipGroupRecord after = new RelationshipGroupRecord( 42 ).initialize( true, (1 << Short.SIZE) + 10, 4, 5, 6, 7, 8 );
        after.setCreated();

        new Command.RelationshipGroupCommand( before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.RelationshipGroupCommand );

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals( relationshipGroupCommand, before, after );
    }

    @RepeatedTest( 200 )
    void shouldReadNodeCommand() throws Exception
    {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        long id = randomEntityId();
        NodeRecord before = new NodeRecord( id );
        NodeRecord after = new NodeRecord( id );
        randomizeBeforeAfterRecords( before, after, this::randomizeNodeRecord, true );

        new Command.NodeCommand( writer(), before, after ).serialize( channel );

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.NodeCommand );

        Command.NodeCommand nodeCommand = (Command.NodeCommand) command;

        // Then
        assertBeforeAndAfterEquals( nodeCommand, before, after );
    }

    @RepeatedTest( 200 )
    void readPropertyCommand() throws IOException
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        long id = randomEntityId();
        PropertyRecord before = new PropertyRecord( id );
        PropertyRecord after = new PropertyRecord( id );
        randomizeBeforeAfterRecords( before, after, this::randomizePropertyRecord, false );

        new Command.PropertyCommand( writer(), before, after ).serialize( channel );

        CommandReader reader = createReader();
        StorageCommand command = reader.read( channel );
        assertTrue( command instanceof Command.PropertyCommand );

        Command.PropertyCommand propertyCommand = (Command.PropertyCommand) command;

        // Then
        assertBeforeAndAfterEquals( propertyCommand, before, after );
    }

    @Test
    void shouldReadSomeCommands() throws Exception
    {
        // GIVEN
        LogCommandSerialization writer = writer();
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        writer.writeNodeCommand( channel, Commands.createNode( 0 ) );
        writer.writeNodeCommand( channel, Commands.createNode( 1 ) );
        writer.writeRelationshipTypeTokenCommand( channel, Commands.createRelationshipTypeToken( 0, 0 ) );
        writer.writeRelationshipCommand( channel, Commands.createRelationship( 0, 0, 1, 0 ) );
        writer.writePropertyKeyTokenCommand( channel, Commands.createPropertyKeyToken( 0, 0 ) );
        writer.writePropertyCommand( channel, Commands.createProperty( 0, PropertyType.SHORT_STRING, 0 ) );
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
        SchemaRule rule = IndexPrototype.forSchema( SchemaDescriptors.forLabel( 1, 2, 3 ) ).withName( "index_" + id ).materialise( id );
        writer().writeSchemaRuleCommand( channel, new Command.SchemaRuleCommand( writer(), before, after, rule ) );

        CommandReader reader = createReader();
        Command.SchemaRuleCommand command = (Command.SchemaRuleCommand) reader.read( channel );

        assertBeforeAndAfterEquals( command, before, after );
    }

    private <R extends AbstractBaseRecord> void randomizeBeforeAfterRecords( R before, R after, Consumer<R> randomizer, boolean canUseSecondaryUnits )
    {
        // Note: the "secondary unit" state cannot be completely randomized since the reader infers some of that state from
        //       other state and to change that would be a format change, so rather play nice with that logic here.

        switch ( randomMode() )
        {
        case CREATE:
        {
            before.clear();
            randomizer.accept( after );
            after.setCreated();
            if ( canUseSecondaryUnits )
            {
                long secondaryUnitId = randomEntityIdOrNull();
                if ( secondaryUnitId != NO_ID )
                {
                    after.setSecondaryUnitIdOnCreate( secondaryUnitId );
                    after.setRequiresSecondaryUnit( true );
                    after.setUseFixedReferences( random.nextBoolean() );
                }
            }
            break;
        }
        case UPDATE:
        {
            randomizer.accept( before );
            randomizer.accept( after );
            if ( canUseSecondaryUnits )
            {
                long secondaryUnitId = randomEntityIdOrNull();
                if ( secondaryUnitId != NO_ID )
                {
                    int situation = random.nextInt( 3 );
                    switch ( situation )
                    {
                    case 0:
                    {
                        // before has none and after requires, i.e. grows into a created secondary unit
                        after.setSecondaryUnitIdOnCreate( secondaryUnitId );
                        after.setRequiresSecondaryUnit( true );
                        break;
                    }
                    case 1:
                    {
                        // before has and after keeps it
                        before.setSecondaryUnitIdOnLoad( secondaryUnitId );
                        before.setRequiresSecondaryUnit( true );
                        after.setSecondaryUnitIdOnLoad( secondaryUnitId );
                        after.setRequiresSecondaryUnit( true );
                        break;
                    }
                    case 2:
                    {
                        // before has and after has none (shrink)
                        before.setSecondaryUnitIdOnLoad( secondaryUnitId );
                        before.setRequiresSecondaryUnit( true );
                        after.setSecondaryUnitIdOnLoad( secondaryUnitId );
                        after.setRequiresSecondaryUnit( false );
                        break;
                    }
                    default:
                        throw new IllegalArgumentException( "invalid situation" );
                    }
                }

                if ( !before.requiresSecondaryUnit() )
                {
                    before.setUseFixedReferences( random.nextBoolean() );
                }
                if ( !after.requiresSecondaryUnit() )
                {
                    after.setUseFixedReferences( random.nextBoolean() );
                }
            }
            break;
        }
        case DELETE:
        {
            randomizer.accept( before );
            after.clear();
            break;
        }
        default:
            throw new IllegalArgumentException( "invalid mode" );
        }
    }

    private void randomizeRelationshipRecord( RelationshipRecord record )
    {
        boolean firstInFirstChain = random.nextBoolean();
        boolean firstInSecondChain = random.nextBoolean();
        record.initialize( true, randomEntityIdOrNull(), randomEntityId(), randomEntityId(), randomRelationshipType(),
                           firstInFirstChain ? randomCount() : randomEntityId(), randomEntityIdOrNull(),
                           firstInSecondChain ? randomCount() : randomEntityId(), randomEntityIdOrNull(), firstInFirstChain, firstInSecondChain );
    }

    private void randomizeRelationshipGroupRecord( RelationshipGroupRecord record )
    {
        record.initialize( true, randomRelationshipType(), randomEntityIdOrNull(), randomEntityIdOrNull(), randomEntityIdOrNull(), randomEntityId(),
                           randomEntityIdOrNull() );
    }

    private void randomizeNodeRecord( NodeRecord record )
    {
        record.initialize( true, randomEntityIdOrNull(), random.nextBoolean(), randomEntityIdOrNull(),
                           randomEntityIdOrNull() /*well, not really but I think it works*/ );
    }

    private void randomizePropertyRecord( PropertyRecord record )
    {
        record.initialize( true, randomEntityIdOrNull(), randomEntityIdOrNull() );
        var block = new PropertyBlock();
        var allocator =
                new StandardDynamicRecordAllocator( new BatchingIdSequence( random.nextLong( 0xFFFFFFFFFFL ) ), GraphDatabaseSettings.DEFAULT_BLOCK_SIZE );
        PropertyStore.encodeValue( block, randomPropertyKey(), random.nextValue(), allocator, allocator, true, CursorContext.NULL,
                                   EmptyMemoryTracker.INSTANCE );
        record.addPropertyBlock( block );
    }

    // Regarding the max IDs below, this test will use the maximum out of all supported formats, even in enterprise edition

    private int randomPropertyKey()
    {
        // All supported formats seemingly have the same max
        return random.nextInt( 1 << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS );
    }

    private Command.Mode randomMode()
    {
        return random.among( Command.Mode.values() );
    }

    private long randomCount()
    {
        return random.nextInt( 1_000_000 );
    }

    private int randomRelationshipType()
    {
        // Randomize between a "small" and "big" representation, because always randomizing with the highest max
        // will produce "small" types quite rarely.
        int max = random.nextBoolean() ? 1 << StandardFormatSettings.RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS : 1 << (Short.SIZE + Byte.SIZE);
        return random.nextInt( max );
    }

    private long randomEntityIdOrNull()
    {
        return random.nextBoolean() ? NO_ID : randomEntityId();
    }

    private long randomEntityId()
    {
        return random.nextLong( 1L << 50 );
    }

    private static SchemaRecord createRandomSchemaRecord()
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

    protected CommandReader createReader()
    {
        return new LogCommandSerializationV4_0();
    }

    static <RECORD extends AbstractBaseRecord> void assertBeforeAndAfterEquals( Command.BaseCommand<RECORD> command, RECORD before, RECORD after )
    {
        assertEqualsIncludingFlags( before, command.getBefore() );
        assertEqualsIncludingFlags( after, command.getAfter() );
    }

    private static <RECORD extends AbstractBaseRecord> void assertEqualsIncludingFlags( RECORD expected, RECORD record )
    {
        assertEquals( expected, record );
        assertEquals( expected.isCreated(), record.isCreated() );
        assertEquals( expected.isSecondaryUnitCreated(), record.isSecondaryUnitCreated() );
        assertEquals( expected.getSecondaryUnitId(), record.getSecondaryUnitId() );
    }

    protected LogCommandSerialization writer()
    {
        return new LogCommandSerializationV4_2();
    }
}
