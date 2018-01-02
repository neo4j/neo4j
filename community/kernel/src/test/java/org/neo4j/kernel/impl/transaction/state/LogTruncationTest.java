/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCountsCommand;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

import static java.lang.reflect.Modifier.isAbstract;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;

/**
 * At any point, a power outage may stop us from writing to the log, which means that, at any point, all our commands
 * need to be able to handle the log ending mid-way through reading it.
 */
public class LogTruncationTest
{
    private final InMemoryLogChannel inMemoryChannel = new InMemoryLogChannel();
    private final LogEntryReader<ReadableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
    private final CommandWriter serializer = new CommandWriter( inMemoryChannel );
    private final LogEntryWriter writer = new LogEntryWriter( inMemoryChannel, serializer );
    /** Stores all known commands, and an arbitrary set of different permutations for them */
    private final Map<Class<?>, Command[]> permutations = new HashMap<>();
    {
        permutations.put( Command.NeoStoreCommand.class,
                new Command[] { new Command.NeoStoreCommand().init( new NeoStoreRecord() ) } );
        permutations.put( Command.NodeCommand.class, new Command[] { new Command.NodeCommand().init( new NodeRecord(
                12l, false, 13l, 13l ), new NodeRecord( 0, false, 0, 0 ) ) } );
        permutations.put( Command.RelationshipCommand.class,
                new Command[] { new Command.RelationshipCommand().init( new RelationshipRecord( 1l, 2l, 3l, 4 ) ) } );
        permutations.put( Command.PropertyCommand.class, new Command[] { new Command.PropertyCommand().init(
                new PropertyRecord( 1, new NodeRecord( 12l, false, 13l, 13 ) ), new PropertyRecord( 1, new NodeRecord(
                        12l, false, 13l, 13 ) ) ) } );
        permutations.put( Command.RelationshipGroupCommand.class,
                new Command[] { new Command.LabelTokenCommand().init( new LabelTokenRecord( 1 ) ) } );
        permutations.put( Command.SchemaRuleCommand.class, new Command[] { new Command.SchemaRuleCommand().init(
                asList( dynamicRecord( 1l, false, true, -1l, 1, "hello".getBytes() ) ),
                asList( dynamicRecord( 1l, true, true, -1l, 1, "hello".getBytes() ) ), new IndexRule( 1, 3, 4,
                        new SchemaIndexProvider.Descriptor( "1", "2" ), null ) ) } );
        permutations
                .put( Command.RelationshipTypeTokenCommand.class,
                        new Command[] { new Command.RelationshipTypeTokenCommand()
                                .init( new RelationshipTypeTokenRecord( 1 ) ) } );
        permutations.put( Command.PropertyKeyTokenCommand.class,
                new Command[] { new Command.PropertyKeyTokenCommand().init( new PropertyKeyTokenRecord( 1 ) ) } );
        permutations.put( Command.LabelTokenCommand.class,
                new Command[] { new Command.LabelTokenCommand().init( new LabelTokenRecord( 1 ) ) } );

        // Index commands
        AddRelationshipCommand addRelationshipCommand = new AddRelationshipCommand();
        addRelationshipCommand.init( 1, 1l, 12345, "some value", 1, 1 );
        permutations.put( AddRelationshipCommand.class, new Command[] { addRelationshipCommand } );

        CreateCommand createCommand = new CreateCommand();
        createCommand.init( 1, IndexEntityType.Relationship.id(), MapUtil.stringMap( "string1", "string 2" ) );
        permutations.put( CreateCommand.class, new Command[] { createCommand } );

        AddNodeCommand addCommand = new AddNodeCommand();
        addCommand.init( 1234, 122l, 2, "value" );
        permutations.put( AddNodeCommand.class, new Command[] { addCommand } );

        DeleteCommand deleteCommand = new DeleteCommand();
        deleteCommand.init( 1, IndexEntityType.Relationship.id() );
        permutations.put( DeleteCommand.class, new Command[] { deleteCommand } );

        RemoveCommand removeCommand = new RemoveCommand();
        removeCommand.init( 1, IndexEntityType.Node.id(), 126, (byte) 3, "the value" );
        permutations.put( RemoveCommand.class, new Command[] { removeCommand } );

        IndexDefineCommand indexDefineCommand = new IndexDefineCommand();
        indexDefineCommand.init( MapUtil.<String, Integer>genericMap(
                "string1", 45, "key1", 2 ), MapUtil.<String, Integer>genericMap( "string", 2 ) );
        permutations.put( IndexDefineCommand.class, new Command[] { indexDefineCommand } );

        // Counts commands
        NodeCountsCommand nodeCounts = new NodeCountsCommand();
        nodeCounts.init( 42, 11 );
        permutations.put( NodeCountsCommand.class, new Command[]{nodeCounts} );
        RelationshipCountsCommand relationshipCounts = new RelationshipCountsCommand();
        relationshipCounts.init( 17, 2, 13, -2 );
        permutations.put( RelationshipCountsCommand.class, new Command[]{relationshipCounts} );
    }

    @Test
    public void testSerializationInFaceOfLogTruncation() throws Exception
    {
        for ( Command cmd : enumerateCommands() )
        {
            assertHandlesLogTruncation( cmd );
        }
    }

    private Iterable<Command> enumerateCommands()
    {
        // We use this reflection approach rather than just iterating over the permutation map to force developers
        // writing new commands to add the new commands to this test. If you came here because of a test failure from
        // missing commands, add all permutations you can think of of the command to the permutations map in the
        // beginning of this class.
        List<Command> commands = new ArrayList<>();
        for ( Class<?> cmd : Command.class.getClasses() )
        {
            if ( Command.class.isAssignableFrom( cmd ) )
            {
                if ( permutations.containsKey( cmd ) )
                {
                    commands.addAll( asList( permutations.get( cmd ) ) );
                }
                else if ( !isAbstract( cmd.getModifiers() ) )
                {
                    throw new AssertionError( "Unknown command type: " + cmd + ", please add missing instantiation to "
                            + "test serialization of this command." );
                }
            }
        }
        for ( Class<?> cmd : IndexCommand.class.getClasses() )
        {
            if ( Command.class.isAssignableFrom( cmd ) )
            {
                if ( permutations.containsKey( cmd ) )
                {
                    commands.addAll( asList( permutations.get( cmd ) ) );
                }
                else if ( !isAbstract( cmd.getModifiers() ) )
                {
                    throw new AssertionError( "Unknown command type: " + cmd + ", please add missing instantiation to "
                            + "test serialization of this command." );
                }
            }
        }
        return commands;
    }

    private void assertHandlesLogTruncation( Command cmd ) throws IOException
    {
        inMemoryChannel.reset();
        writer.serialize( new PhysicalTransactionRepresentation( Arrays.asList( cmd ) ) );
        int bytesSuccessfullyWritten = inMemoryChannel.writerPosition();
        try
        {
            LogEntry logEntry = logEntryReader.readLogEntry( inMemoryChannel );
            Command command = ((LogEntryCommand) logEntry).getXaCommand();
            assertEquals( cmd, command );
        }
        catch ( Exception e )
        {
            throw new AssertionError( "Failed to deserialize " + cmd.toString() + ", because: ", e );
        }
        bytesSuccessfullyWritten--;
        while ( bytesSuccessfullyWritten-- > 0 )
        {
            inMemoryChannel.reset();
            writer.serialize( new PhysicalTransactionRepresentation( Arrays.asList( cmd ) ) );
            inMemoryChannel.truncateTo( bytesSuccessfullyWritten );
            LogEntry deserialized = logEntryReader.readLogEntry( inMemoryChannel );
            assertNull( "Deserialization did not detect log truncation!" +
                    "Record: " + cmd + ", deserialized: " + deserialized, deserialized );
        }
    }

    @Test
    public void testInMemoryLogChannel() throws Exception
    {
        InMemoryLogChannel channel = new InMemoryLogChannel();
        for ( int i = 0; i < 25; i++ )
        {
            channel.putInt( i );
        }
        for ( int i = 0; i < 25; i++ )
        {
            assertEquals( i, channel.getInt() );
        }
        channel.reset();
        for ( long i = 0; i < 12; i++ )
        {
            channel.putLong( i );
        }
        for ( long i = 0; i < 12; i++ )
        {
            assertEquals( i, channel.getLong() );
        }
        channel.reset();
        for ( long i = 0; i < 8; i++ )
        {
            channel.putLong( i );
            channel.putInt( (int) i );
        }
        for ( long i = 0; i < 8; i++ )
        {
            assertEquals( i, channel.getLong() );
            assertEquals( i, channel.getInt() );
        }
        channel.close();
    }
}
