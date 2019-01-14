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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
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
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.StorageCommand;

import static java.lang.reflect.Modifier.isAbstract;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;

/**
 * At any point, a power outage may stop us from writing to the log, which means that, at any point, all our commands
 * need to be able to handle the log ending mid-way through reading it.
 */
public class LogTruncationTest
{
    private final InMemoryClosableChannel inMemoryChannel = new InMemoryClosableChannel();
    private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
    private final LogEntryWriter writer = new LogEntryWriter( inMemoryChannel );
    /** Stores all known commands, and an arbitrary set of different permutations for them */
    private final Map<Class<?>, Command[]> permutations = new HashMap<>();
    {
        NeoStoreRecord after = new NeoStoreRecord();
        after.setNextProp( 42 );
        permutations.put( Command.NeoStoreCommand.class,
                new Command[] { new Command.NeoStoreCommand( new NeoStoreRecord(), after ) } );
        permutations.put( Command.NodeCommand.class, new Command[] { new Command.NodeCommand(
                new NodeRecord( 12L, false, 13L, 13L ), new NodeRecord( 0, false, 0, 0 ) ) } );
        permutations.put( Command.RelationshipCommand.class,
                new Command[] { new Command.RelationshipCommand( new RelationshipRecord( 1L ),
                        new RelationshipRecord( 1L, 2L, 3L, 4 ) ) } );
        permutations.put( Command.PropertyCommand.class, new Command[] { new Command.PropertyCommand(
                new PropertyRecord( 1, new NodeRecord( 12L, false, 13L, 13 ) ), new PropertyRecord( 1, new NodeRecord(
                        12L, false, 13L, 13 ) ) ) } );
        permutations.put( Command.RelationshipGroupCommand.class,
                new Command[] { new Command.LabelTokenCommand( new LabelTokenRecord( 1 ),
                        createLabelTokenRecord( 1 ) ) } );
        permutations.put( Command.SchemaRuleCommand.class, new Command[] { new Command.SchemaRuleCommand(
                singletonList( dynamicRecord( 1L, false, true, -1L, 1, "hello".getBytes() ) ),
                singletonList( dynamicRecord( 1L, true, true, -1L, 1, "hello".getBytes() ) ),
                IndexRule.indexRule( 1, SchemaIndexDescriptorFactory.forLabel( 3, 4 ),
                        new IndexProvider.Descriptor( "1", "2" ) ) ) } );
        permutations
                .put( Command.RelationshipTypeTokenCommand.class,
                        new Command[] { new Command.RelationshipTypeTokenCommand(
                                new RelationshipTypeTokenRecord( 1 ), createRelationshipTypeTokenRecord( 1 ) ) } );
        permutations.put( Command.PropertyKeyTokenCommand.class,
                new Command[] { new Command.PropertyKeyTokenCommand( new PropertyKeyTokenRecord( 1 ),
                        createPropertyKeyTokenRecord( 1 ) ) } );
        permutations.put( Command.LabelTokenCommand.class,
                new Command[] { new Command.LabelTokenCommand( new LabelTokenRecord( 1 ),
                        createLabelTokenRecord( 1 ) ) } );

        // Index commands
        AddRelationshipCommand addRelationshipCommand = new AddRelationshipCommand();
        addRelationshipCommand.init( 1, 1L, 12345, "some value", 1, 1 );
        permutations.put( AddRelationshipCommand.class, new Command[] { addRelationshipCommand } );

        CreateCommand createCommand = new CreateCommand();
        createCommand.init( 1, IndexEntityType.Relationship.id(), MapUtil.stringMap( "string1", "string 2" ) );
        permutations.put( CreateCommand.class, new Command[] { createCommand } );

        AddNodeCommand addCommand = new AddNodeCommand();
        addCommand.init( 1234, 122L, 2, "value" );
        permutations.put( AddNodeCommand.class, new Command[] { addCommand } );

        DeleteCommand deleteCommand = new DeleteCommand();
        deleteCommand.init( 1, IndexEntityType.Relationship.id() );
        permutations.put( DeleteCommand.class, new Command[] { deleteCommand } );

        RemoveCommand removeCommand = new RemoveCommand();
        removeCommand.init( 1, IndexEntityType.Node.id(), 126, (byte) 3, "the value" );
        permutations.put( RemoveCommand.class, new Command[] { removeCommand } );

        IndexDefineCommand indexDefineCommand = new IndexDefineCommand();
        indexDefineCommand.init( MapUtil.genericMap(
                "string1", 45, "key1", 2 ), MapUtil.genericMap( "string", 2 ) );
        permutations.put( IndexDefineCommand.class, new Command[] { indexDefineCommand } );

        // Counts commands
        permutations.put( NodeCountsCommand.class, new Command[]{new NodeCountsCommand( 42, 11 )} );
        permutations.put( RelationshipCountsCommand.class,
                new Command[]{new RelationshipCountsCommand( 17, 2, 13, -2 )} );
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
        writer.serialize( new PhysicalTransactionRepresentation( singletonList( cmd ) ) );
        int bytesSuccessfullyWritten = inMemoryChannel.writerPosition();
        try
        {
            LogEntry logEntry = logEntryReader.readLogEntry( inMemoryChannel );
            StorageCommand command = ((LogEntryCommand) logEntry).getCommand();
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
            writer.serialize( new PhysicalTransactionRepresentation( singletonList( cmd ) ) );
            inMemoryChannel.truncateTo( bytesSuccessfullyWritten );
            LogEntry deserialized = logEntryReader.readLogEntry( inMemoryChannel );
            assertNull( "Deserialization did not detect log truncation!" +
                    "Record: " + cmd + ", deserialized: " + deserialized, deserialized );
        }
    }

    @Test
    public void testInMemoryLogChannel() throws Exception
    {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
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

    private LabelTokenRecord createLabelTokenRecord( int id )
    {
        LabelTokenRecord labelTokenRecord = new LabelTokenRecord( id );
        labelTokenRecord.setInUse( true );
        labelTokenRecord.setNameId( 333 );
        labelTokenRecord.addNameRecord( new DynamicRecord( 43 ) );
        return labelTokenRecord;
    }

    private RelationshipTypeTokenRecord createRelationshipTypeTokenRecord( int id )
    {
        RelationshipTypeTokenRecord relationshipTypeTokenRecord = new RelationshipTypeTokenRecord( id );
        relationshipTypeTokenRecord.setInUse( true );
        relationshipTypeTokenRecord.setNameId( 333 );
        relationshipTypeTokenRecord.addNameRecord( new DynamicRecord( 43 ) );
        return relationshipTypeTokenRecord;
    }

    private PropertyKeyTokenRecord createPropertyKeyTokenRecord( int id )
    {
        PropertyKeyTokenRecord propertyKeyTokenRecord = new PropertyKeyTokenRecord( id );
        propertyKeyTokenRecord.setInUse( true );
        propertyKeyTokenRecord.setNameId( 333 );
        propertyKeyTokenRecord.addNameRecord( new DynamicRecord( 43 ) );
        return propertyKeyTokenRecord;
    }
}
