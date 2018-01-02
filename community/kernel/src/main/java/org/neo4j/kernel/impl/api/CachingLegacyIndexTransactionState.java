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
package org.neo4j.kernel.impl.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.transaction.command.Command;

class CachingLegacyIndexTransactionState implements LegacyIndexTransactionState
{
    private Map<String,LegacyIndex> nodeLegacyIndexChanges;
    private Map<String,LegacyIndex> relationshipLegacyIndexChanges;
    private final LegacyIndexTransactionState txState;

    CachingLegacyIndexTransactionState( LegacyIndexTransactionState txState )
    {
        this.txState = txState;
    }

    @Override
    public void clear()
    {
        txState.clear();
        if ( nodeLegacyIndexChanges != null && !nodeLegacyIndexChanges.isEmpty() )
        {
            nodeLegacyIndexChanges.clear();
        }
        if ( relationshipLegacyIndexChanges != null && !relationshipLegacyIndexChanges.isEmpty() )
        {
            relationshipLegacyIndexChanges.clear();
        }
    }

    @Override
    public LegacyIndex nodeChanges( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        if ( nodeLegacyIndexChanges == null )
        {
            nodeLegacyIndexChanges = new HashMap<>();
        }
        LegacyIndex changes = nodeLegacyIndexChanges.get( indexName );
        if ( changes == null )
        {
            nodeLegacyIndexChanges.put( indexName, changes = txState.nodeChanges( indexName ) );
        }
        return changes;
    }

    @Override
    public LegacyIndex relationshipChanges( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        if ( relationshipLegacyIndexChanges == null )
        {
            relationshipLegacyIndexChanges = new HashMap<>();
        }
        LegacyIndex changes = relationshipLegacyIndexChanges.get( indexName );
        if ( changes == null )
        {
            relationshipLegacyIndexChanges.put( indexName, changes = txState.relationshipChanges( indexName ) );
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
    public void extractCommands( Collection<Command> target ) throws TransactionFailureException
    {
        txState.extractCommands( target );
    }
}
