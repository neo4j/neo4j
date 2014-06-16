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
package org.neo4j.kernel.impl.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;

import static org.neo4j.graphdb.index.IndexManager.PROVIDER;

public class LegacyIndexApplier extends NeoCommandHandler.Adapter
{
    public interface ProviderLookup
    {
        IndexImplementation lookup( String name );
    }

    private IndexDefineCommand defineCommand;
    private final ProviderLookup providerLookup;
    private final Map<String, NeoCommandHandler> providerAppliers = new HashMap<>();
    private final IndexConfigStore indexConfigStore;
    private final boolean recovery;

    public LegacyIndexApplier( IndexConfigStore indexConfigStore, ProviderLookup providerLookup, boolean recovery )
    {
        this.indexConfigStore = indexConfigStore;
        this.providerLookup = providerLookup;
        this.recovery = recovery;
    }

    private NeoCommandHandler applier( IndexCommand command ) throws IOException
    {
        byte nameId = command.getIndexNameId();
        String indexName = defineCommand.getIndexName( nameId );
        NeoCommandHandler applier = providerAppliers.get( indexName );
        if ( applier == null )
        {
            IndexEntityType entityType = IndexEntityType.byId( command.getEntityType() );
            String providerName = indexConfigStore.get( entityType.entityClass(), indexName ).get( PROVIDER );
            applier = providerLookup.lookup( providerName ).newApplier( recovery );
            applier.visitIndexDefineCommand( defineCommand );
            providerAppliers.put( indexName, applier );
        }
        return applier;
    }

    @Override
    public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
    {
        return applier( command ).visitIndexAddNodeCommand( command );
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
    {
        return applier( command ).visitIndexAddRelationshipCommand( command );
    }

    @Override
    public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
    {
        return applier( command ).visitIndexRemoveCommand( command );
    }

    @Override
    public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
    {
        return applier( command ).visitIndexDeleteCommand( command );
    }

    @Override
    public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
    {
        indexConfigStore.setIfNecessary( IndexEntityType.byId( command.getEntityType() ).entityClass(),
                defineCommand.getIndexName( command.getIndexNameId() ), command.getConfig() );
        return applier( command ).visitIndexCreateCommand( command );
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        this.defineCommand = command;
        return true;
    }

    @Override
    public void close()
    {
        for ( NeoCommandHandler applier : providerAppliers.values() )
        {
            applier.close();
        }
    }
}
