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
package org.neo4j.kernel.impl.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.storageengine.api.StorageCommand;

public class CachingExplicitIndexTransactionState implements ExplicitIndexTransactionState
{
    private Map<String,ExplicitIndex> nodeExplicitIndexChanges;
    private Map<String,ExplicitIndex> relationshipExplicitIndexChanges;
    private final ExplicitIndexTransactionState txState;

    public CachingExplicitIndexTransactionState( ExplicitIndexTransactionState txState )
    {
        this.txState = txState;
    }

    @Override
    public ExplicitIndex nodeChanges( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        if ( nodeExplicitIndexChanges == null )
        {
            nodeExplicitIndexChanges = new HashMap<>();
        }
        ExplicitIndex changes = nodeExplicitIndexChanges.get( indexName );
        if ( changes == null )
        {
            nodeExplicitIndexChanges.put( indexName, changes = txState.nodeChanges( indexName ) );
        }
        return changes;
    }

    @Override
    public ExplicitIndex relationshipChanges( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        if ( relationshipExplicitIndexChanges == null )
        {
            relationshipExplicitIndexChanges = new HashMap<>();
        }
        ExplicitIndex changes = relationshipExplicitIndexChanges.get( indexName );
        if ( changes == null )
        {
            relationshipExplicitIndexChanges.put( indexName, changes = txState.relationshipChanges( indexName ) );
        }
        return changes;
    }

    @Override
    public void createIndex( IndexEntityType node, String name, Map<String,String> config )
    {
        txState.createIndex( node, name, config );
    }

    @Override
    public void deleteIndex( IndexEntityType entityType, String indexName )
    {
        txState.deleteIndex( entityType, indexName );
    }

    @Override
    public boolean hasChanges()
    {
        return txState.hasChanges();
    }

    @Override
    public void extractCommands( Collection<StorageCommand> target ) throws TransactionFailureException
    {
        txState.extractCommands( target );
    }

    @Override
    public boolean checkIndexExistence( IndexEntityType entityType, String indexName, Map<String,String> config )
    {
        return txState.checkIndexExistence( entityType, indexName, config );
    }
}
