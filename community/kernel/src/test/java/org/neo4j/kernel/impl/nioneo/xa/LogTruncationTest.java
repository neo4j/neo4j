/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.nioneo.store.DynamicRecord.dynamicRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
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
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.CommandWriter;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;

/**
 * At any point, a power outage may stop us from writing to the log, which means that, at any point, all our commands
 * need to be able to handle the log ending mid-way through reading it.
 */
public class LogTruncationTest
{
    private final InMemoryLogChannel inMemoryChannel = new InMemoryLogChannel();
    private final VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader(
            CommandReaderFactory.DEFAULT );
    private final CommandWriter serializer = new CommandWriter( inMemoryChannel );
    private final LogEntryWriterv1 writer = new LogEntryWriterv1( inMemoryChannel, serializer );
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
        addRelationshipCommand.init( (byte) 1, 1l, (byte) 1, "some value", 1, 1 );
        permutations.put( AddRelationshipCommand.class, new Command[] { addRelationshipCommand } );

        CreateCommand createCommand = new CreateCommand();
        createCommand.init( (byte) 1, IndexEntityType.Relationship.id(), MapUtil.stringMap( "string1", "string 2" ) );
        permutations.put( CreateCommand.class, new Command[] { createCommand } );

        AddNodeCommand addCommand = new AddNodeCommand();
        addCommand.init( (byte) 1, 122l, (byte) 2, "value" );
        permutations.put( AddNodeCommand.class, new Command[] { addCommand } );

        DeleteCommand deleteCommand = new DeleteCommand();
        deleteCommand.init( (byte) 1, IndexEntityType.Relationship.id() );
        permutations.put( DeleteCommand.class, new Command[] { deleteCommand } );

        RemoveCommand removeCommand = new RemoveCommand();
        removeCommand.init( (byte) 1, IndexEntityType.Node.id(), 126, (byte) 3, "the value" );
        permutations.put( RemoveCommand.class, new Command[] { removeCommand } );

        IndexDefineCommand indexDefineCommand = new IndexDefineCommand();
        indexDefineCommand.init( MapUtil.<String, Byte>genericMap( "string1", (byte) 45, "key1", (byte) 2 ), MapUtil.<String, Byte>genericMap( "string", (byte) 2 ) );
        permutations.put( IndexDefineCommand.class, new Command[] { indexDefineCommand } );
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
                else
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
                else
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
        writer.writeCommandEntry( cmd );
        int bytesSuccessfullyWritten = inMemoryChannel.writerPosition();
        try
        {
            assertEquals( cmd, ((LogEntry.Command) logEntryReader.readLogEntry( inMemoryChannel )).getXaCommand() );
        }
        catch ( Exception e )
        {
            throw new AssertionError( "Failed to deserialize " + cmd.toString() + ", because: ", e );
        }
        bytesSuccessfullyWritten--;
        while ( bytesSuccessfullyWritten-- > 0 )
        {
            inMemoryChannel.reset();
            writer.writeCommandEntry( cmd );
            inMemoryChannel.truncateTo( bytesSuccessfullyWritten );
            LogEntry.Command deserialized = ((LogEntry.Command) logEntryReader.readLogEntry( inMemoryChannel ));
            assertNull( "Deserialization did not detect log truncation! Record: " + cmd + ", deserialized: "
                    + deserialized, deserialized );
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
