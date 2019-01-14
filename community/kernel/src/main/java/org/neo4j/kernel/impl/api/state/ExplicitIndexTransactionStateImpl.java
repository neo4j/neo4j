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
package org.neo4j.kernel.impl.api.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.spi.explicitindex.ExplicitIndexProviderTransaction;
import org.neo4j.kernel.spi.explicitindex.IndexCommandFactory;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.kernel.impl.index.ExplicitIndexStore.assertConfigMatches;

/**
 * Provides access to {@link ExplicitIndex indexes}. Holds transaction state for all providers in a transaction.
 * A equivalent to TransactionRecordState, but for explicit indexes.
 *
 * @see TransactionRecordState
 */
public class ExplicitIndexTransactionStateImpl implements ExplicitIndexTransactionState, IndexCommandFactory
{
    private final Map<String,ExplicitIndexProviderTransaction> transactions = new HashMap<>();
    private final IndexConfigStore indexConfigStore;
    private final Function<String,IndexImplementation> providerLookup;

    // Commands
    private IndexDefineCommand defineCommand;
    private final Map<String, List<IndexCommand>> nodeCommands = new HashMap<>();
    private final Map<String, List<IndexCommand>> relationshipCommands = new HashMap<>();

    public ExplicitIndexTransactionStateImpl( IndexConfigStore indexConfigStore,
            Function<String,IndexImplementation> providerLookup )
    {
        this.indexConfigStore = indexConfigStore;
        this.providerLookup = providerLookup;
    }

    @Override
    public ExplicitIndex nodeChanges( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        Map<String, String> configuration = indexConfigStore.get( Node.class, indexName );
        if ( configuration == null )
        {
            throw new ExplicitIndexNotFoundKernelException( "Node index '" + indexName + " not found" );
        }
        String providerName = configuration.get( IndexManager.PROVIDER );
        IndexImplementation provider = providerLookup.apply( providerName );
        ExplicitIndexProviderTransaction transaction =
                transactions.computeIfAbsent( providerName, k -> provider.newTransaction( this ) );
        return transaction.nodeIndex( indexName, configuration );
    }

    @Override
    public ExplicitIndex relationshipChanges( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        Map<String, String> configuration = indexConfigStore.get( Relationship.class, indexName );
        if ( configuration == null )
        {
            throw new ExplicitIndexNotFoundKernelException( "Relationship index '" + indexName + " not found" );
        }
        String providerName = configuration.get( IndexManager.PROVIDER );
        IndexImplementation provider = providerLookup.apply( providerName );
        ExplicitIndexProviderTransaction transaction = transactions.get( providerName );
        if ( transaction == null )
        {
            transactions.put( providerName, transaction = provider.newTransaction( this ) );
        }
        return transaction.relationshipIndex( indexName, configuration );
    }

    @Override
    public void extractCommands( Collection<StorageCommand> target )
    {
        if ( defineCommand != null )
        {
            target.add( defineCommand );
            extractCommands( target, nodeCommands );
            extractCommands( target, relationshipCommands );
        }

        for ( ExplicitIndexProviderTransaction providerTransaction : transactions.values() )
        {
            providerTransaction.close();
        }
    }

    private void extractCommands( Collection<StorageCommand> target, Map<String, List<IndexCommand>> commandMap )
    {
        if ( commandMap != null )
        {
            for ( List<IndexCommand> commands : commandMap.values() )
            {
                target.addAll( commands );
            }
        }
    }

    // Methods for adding commands
    private IndexDefineCommand definitions()
    {
        if ( defineCommand == null )
        {
            defineCommand = new IndexDefineCommand();
        }
        return defineCommand;
    }

    private void addCommand( String indexName, IndexCommand command )
    {
        addCommand( indexName, command, false );
    }

    private void addCommand( String indexName, IndexCommand command, boolean clearFirst )
    {
        List<IndexCommand> commands = null;
        if ( command.getEntityType() == IndexEntityType.Node.id() )
        {
            commands = nodeCommands.computeIfAbsent( indexName, k -> new ArrayList<>() );
        }
        else if ( command.getEntityType() == IndexEntityType.Relationship.id() )
        {
            commands = relationshipCommands.computeIfAbsent( indexName, k -> new ArrayList<>() );
        }
        else
        {
            throw new IllegalArgumentException( "" + command.getEntityType() );
        }

        if ( clearFirst )
        {
            commands.clear();
        }

        commands.add( command );
    }

    @Override
    public void addNode( String indexName, long id, String key, Object value )
    {
        AddNodeCommand command = new AddNodeCommand();
        command.init( definitions().getOrAssignIndexNameId( indexName ),
                id, definitions().getOrAssignKeyId( key ), value );
        addCommand( indexName, command );
    }

    @Override
    public void addRelationship( String indexName, long id, String key, Object value,
            long startNode, long endNode )
    {
        AddRelationshipCommand command = new AddRelationshipCommand();
        command.init( definitions().getOrAssignIndexNameId( indexName ),
                id, definitions().getOrAssignKeyId( key ), value, startNode, endNode );
        addCommand( indexName, command );
    }

    @Override
    public void removeNode( String indexName, long id,
            String keyOrNull, Object valueOrNull )
    {
        RemoveCommand command = new RemoveCommand();
        command.init( definitions().getOrAssignIndexNameId( indexName ),
                IndexEntityType.Node.id(), id, definitions().getOrAssignKeyId( keyOrNull ), valueOrNull );
        addCommand( indexName, command );
    }

    @Override
    public void removeRelationship( String indexName, long id,
            String keyOrNull, Object valueOrNull )
    {
        RemoveCommand command = new RemoveCommand();
        command.init( definitions().getOrAssignIndexNameId( indexName ),
                IndexEntityType.Relationship.id(), id, definitions().getOrAssignKeyId( keyOrNull ), valueOrNull );
        addCommand( indexName, command );
    }

    @Override
    public void deleteIndex( IndexEntityType entityType, String indexName )
    {
        DeleteCommand command = new DeleteCommand();
        command.init( definitions().getOrAssignIndexNameId( indexName ), entityType.id() );
        addCommand( indexName, command, true );
    }

    @Override
    public void createIndex( IndexEntityType entityType, String indexName, Map<String, String> config )
    {
        CreateCommand command = new CreateCommand();
        command.init( definitions().getOrAssignIndexNameId( indexName ), entityType.id(), config );
        addCommand( indexName, command );
    }

    @Override
    public boolean hasChanges()
    {
        return defineCommand != null;
    }

    @Override
    public boolean checkIndexExistence( IndexEntityType entityType, String indexName, Map<String,String> config )
    {
        Map<String, String> configuration = indexConfigStore.get( entityType.entityClass(), indexName );
        if ( configuration == null )
        {
            return false;
        }

        String providerName = configuration.get( IndexManager.PROVIDER );
        IndexImplementation provider = providerLookup.apply( providerName );
        assertConfigMatches( provider, indexName, configuration, config );
        return true;
    }
}
