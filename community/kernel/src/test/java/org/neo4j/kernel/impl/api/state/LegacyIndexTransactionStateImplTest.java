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
package org.neo4j.kernel.impl.api.state;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.function.Function;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.transaction.command.Command;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LegacyIndexTransactionStateImplTest
{
    @Test
    public void tracksNodeCommands()
    {
        LegacyIndexTransactionStateImpl state = newLegacyIndexTxState();

        state.addNode( "index1", 1, "key1", "value1" );
        state.removeNode( "index1", 1, "key2", "value2" );
        state.addNode( "index1", 2, "key1", "value3" );
        state.addNode( "index1", 3, "key2", "value4" );
        state.removeNode( "index2", 4, "key1", "value5" );

        IndexDefineCommand indexDefinedCommand = new IndexDefineCommand();
        indexDefinedCommand.getOrAssignIndexNameId( "index1" );
        indexDefinedCommand.getOrAssignIndexNameId( "index2" );
        indexDefinedCommand.getOrAssignKeyId( "key1" );
        indexDefinedCommand.getOrAssignKeyId( "key2" );

        Set<Command> expectedCommands = new HashSet<>( Arrays.asList(
                indexDefinedCommand,
                addNode( 1, 1, 1, "value1" ),
                removeNode( 1, 1, 2, "value2" ),
                addNode( 1, 2, 1, "value3" ),
                addNode( 1, 3, 2, "value4" ),
                removeNode( 2, 4, 1, "value5" )
        ) );
        assertEquals( expectedCommands, extractCommands( state ) );
    }

    @Test
    public void tracksRelationshipCommands()
    {
        LegacyIndexTransactionStateImpl state = newLegacyIndexTxState();

        state.removeRelationship( "index1", 1, "key1", "value1" );
        state.addRelationship( "index1", 1, "key2", "value2", 11, 11 );
        state.removeRelationship( "index1", 2, "key1", "value3" );
        state.addRelationship( "index1", 3, "key2", "value4", 22, 22 );
        state.addRelationship( "index2", 4, "key1", "value5", 33, 33 );

        IndexDefineCommand indexDefinedCommand = new IndexDefineCommand();
        indexDefinedCommand.getOrAssignIndexNameId( "index1" );
        indexDefinedCommand.getOrAssignIndexNameId( "index2" );
        indexDefinedCommand.getOrAssignKeyId( "key1" );
        indexDefinedCommand.getOrAssignKeyId( "key2" );

        Set<Command> expectedCommands = new HashSet<>( Arrays.asList(
                indexDefinedCommand,
                removeRelationship( 1, 1, 1, "value1" ),
                addRelationship( 1, 1, 2, "value2", 11, 11 ),
                removeRelationship( 1, 2, 1, "value3" ),
                addRelationship( 1, 3, 2, "value4", 22, 22 ),
                addRelationship( 2, 4, 1, "value5", 33, 33 )
        ) );
        assertEquals( expectedCommands, extractCommands( state ) );
    }

    @Test
    public void nodeIndexDeletionRemovesCommands()
    {
        LegacyIndexTransactionStateImpl state = newLegacyIndexTxState();

        state.addNode( "index", 1, "key", "value1" );
        state.addNode( "index", 2, "key", "value2" );
        state.removeNode( "index", 3, "key", "value3" );

        state.deleteIndex( IndexEntityType.Node, "index" );

        IndexDefineCommand indexDefinedCommand = new IndexDefineCommand();
        indexDefinedCommand.getOrAssignIndexNameId( "index" );
        indexDefinedCommand.getOrAssignKeyId( "key" );

        IndexCommand.DeleteCommand delete = new IndexCommand.DeleteCommand();
        delete.init( 1, IndexEntityType.Node.id() );

        Set<Command> expectedCommands = new HashSet<>( Arrays.asList( indexDefinedCommand, delete ) );
        assertEquals( expectedCommands, extractCommands( state ) );
    }

    @Test
    public void relationshipIndexDeletionRemovesCommands()
    {
        LegacyIndexTransactionStateImpl state = newLegacyIndexTxState();

        state.removeRelationship( "index", 1, "key", "value1" );
        state.addRelationship( "index", 2, "key", "value2", 11, 11 );
        state.addRelationship( "index", 3, "key", "value3", 22, 22 );

        state.deleteIndex( IndexEntityType.Relationship, "index" );

        IndexDefineCommand indexDefinedCommand = new IndexDefineCommand();
        indexDefinedCommand.getOrAssignIndexNameId( "index" );
        indexDefinedCommand.getOrAssignKeyId( "key" );

        IndexCommand.DeleteCommand delete = new IndexCommand.DeleteCommand();
        delete.init( 1, IndexEntityType.Relationship.id() );

        Set<Command> expectedCommands = new HashSet<>( Arrays.asList( indexDefinedCommand, delete ) );
        assertEquals( expectedCommands, extractCommands( state ) );
    }

    @Test
    public void removalOfNodeIndexDoesNotClearRelationshipCommandsForRelationshipIndexWithSameName()
    {
        LegacyIndexTransactionStateImpl state = newLegacyIndexTxState();

        state.addNode( "index", 1, "key", "value" );
        state.addRelationship( "index", 1, "key", "value", 11, 11 );
        state.deleteIndex( IndexEntityType.Node, "index" );

        IndexDefineCommand indexDefinedCommand = new IndexDefineCommand();
        indexDefinedCommand.getOrAssignIndexNameId( "index" );
        indexDefinedCommand.getOrAssignKeyId( "key" );

        IndexCommand.DeleteCommand delete = new IndexCommand.DeleteCommand();
        delete.init( 1, IndexEntityType.Node.id() );

        Set<Command> expectedCommands = new HashSet<>( Arrays.asList(
                indexDefinedCommand,
                delete,
                addRelationship( 1, 1, 1, "value", 11, 11 )
        ) );
        assertEquals( expectedCommands, extractCommands( state ) );
    }

    @Test
    public void removalOfRelationshipIndexDoesNotClearNodeCommandsForNodeIndexWithSameName()
    {
        LegacyIndexTransactionStateImpl state = newLegacyIndexTxState();

        state.addNode( "index", 1, "key", "value" );
        state.addRelationship( "index", 1, "key", "value", 11, 11 );
        state.deleteIndex( IndexEntityType.Relationship, "index" );

        IndexDefineCommand indexDefinedCommand = new IndexDefineCommand();
        indexDefinedCommand.getOrAssignIndexNameId( "index" );
        indexDefinedCommand.getOrAssignKeyId( "key" );

        IndexCommand.DeleteCommand delete = new IndexCommand.DeleteCommand();
        delete.init( 1, IndexEntityType.Relationship.id() );

        Set<Command> expectedCommands = new HashSet<>( Arrays.asList(
                indexDefinedCommand,
                delete,
                addNode( 1, 1, 1, "value" )
        ) );
        assertEquals( expectedCommands, extractCommands( state ) );
    }

    private static Set<Command> extractCommands( LegacyIndexTransactionStateImpl state )
    {
        Set<Command> commands = new HashSet<>();
        state.extractCommands( commands );
        return commands;
    }

    private static Command addNode( int index, long id, int key, Object value )
    {
        IndexCommand.AddNodeCommand command = new IndexCommand.AddNodeCommand();
        command.init( index, id, key, value );
        return command;
    }

    private static Command addRelationship( int index, long id, int key, Object value, long startNode, long endNode )
    {
        IndexCommand.AddRelationshipCommand command = new IndexCommand.AddRelationshipCommand();
        command.init( index, id, key, value, startNode, endNode );
        return command;
    }

    private static Command removeNode( int index, long id, int key, Object value )
    {
        IndexCommand.RemoveCommand command = new IndexCommand.RemoveCommand();
        command.init( index, IndexEntityType.Node.id(), id, key, value );
        return command;
    }

    private static Command removeRelationship( int index, long id, int key, Object value )
    {
        IndexCommand.RemoveCommand command = new IndexCommand.RemoveCommand();
        command.init( index, IndexEntityType.Relationship.id(), id, key, value );
        return command;
    }

    private static LegacyIndexTransactionStateImpl newLegacyIndexTxState()
    {
        IndexConfigStore indexConfigStore = mock( IndexConfigStore.class );
        when( indexConfigStore.get( eq( Node.class ), anyString() ) )
                .thenReturn( singletonMap( IndexManager.PROVIDER, "test" ) );
        when( indexConfigStore.get( eq( Relationship.class ), anyString() ) )
                .thenReturn( singletonMap( IndexManager.PROVIDER, "test" ) );

        Function<String,IndexImplementation> providerLookup = new Function<String,IndexImplementation>()
        {
            @Override
            public IndexImplementation apply( String s ) throws RuntimeException
            {
                return mock( IndexImplementation.class );
            }
        };

        return new LegacyIndexTransactionStateImpl( indexConfigStore, providerLookup );
    }
}
