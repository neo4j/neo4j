/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NeoStoreCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.command.Command.TokenCommand;

public class HighIdTransactionApplier implements NeoCommandHandler
{
    private final NeoCommandHandler delegate;
    private final NeoStore neoStore;
    private final Map<CommonAbstractStore,HighId> highIds = new HashMap<>();

    public HighIdTransactionApplier( NeoCommandHandler delegate, NeoStore neoStore )
    {
        this.delegate = delegate;
        this.neoStore = neoStore;
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command ) throws IOException
    {
        NodeStore nodeStore = neoStore.getNodeStore();
        track( nodeStore, command );
        track( nodeStore.getDynamicLabelStore(), command.getAfter().getDynamicLabelRecords() );
        return delegate.visitNodeCommand( command );
    }

    @Override
    public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
    {
        track( neoStore.getRelationshipStore(), command );
        return delegate.visitRelationshipCommand( command );
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
    {
        PropertyStore propertyStore = neoStore.getPropertyStore();
        track( propertyStore, command );
        for ( PropertyBlock block : command.getAfter() )
        {
            switch ( block.getType() )
            {
            case STRING:
                track( propertyStore.getStringStore(), block.getValueRecords() );
                break;
            case ARRAY:
                track( propertyStore.getArrayStore(), block.getValueRecords() );
                break;
            default:
                // Not needed, no dynamic records then
                break;
            }
        }
        return delegate.visitPropertyCommand( command );
    }

    @Override
    public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException
    {
        track( neoStore.getRelationshipGroupStore(), command );
        return delegate.visitRelationshipGroupCommand( command );
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException
    {
        trackToken( neoStore.getRelationshipTypeTokenStore(), command );
        return delegate.visitRelationshipTypeTokenCommand( command );
    }

    @Override
    public boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException
    {
        trackToken( neoStore.getLabelTokenStore(), command );
        return delegate.visitLabelTokenCommand( command );
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException
    {
        trackToken( neoStore.getPropertyKeyTokenStore(), command );
        return delegate.visitPropertyKeyTokenCommand( command );
    }

    @Override
    public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
    {
        SchemaStore schemaStore = neoStore.getSchemaStore();
        for ( DynamicRecord record : command.getRecordsAfter() )
        {
            track( schemaStore, record.getId() );
        }
        return delegate.visitSchemaRuleCommand( command );
    }

    @Override
    public boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException
    {
        delegate.visitNeoStoreCommand( command );
        return false;
    }

    @Override
    public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
    {
        return delegate.visitIndexAddNodeCommand( command );
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
    {
        return delegate.visitIndexAddRelationshipCommand( command );
    }

    @Override
    public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
    {
        return delegate.visitIndexRemoveCommand( command );
    }

    @Override
    public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
    {
        return delegate.visitIndexDeleteCommand( command );
    }

    @Override
    public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
    {
        return delegate.visitIndexCreateCommand( command );
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        return delegate.visitIndexDefineCommand( command );
    }

    @Override
    public boolean visitNodeCountsCommand( NodeCountsCommand command ) throws IOException
    {
        return delegate.visitNodeCountsCommand( command );
    }

    @Override
    public boolean visitRelationshipCountsCommand( RelationshipCountsCommand command ) throws IOException
    {
        return delegate.visitRelationshipCountsCommand( command );
    }

    @Override
    public void apply()
    {
        delegate.apply();
        // Notifies the stores about the recovered ids and will bump those high ids atomically if
        // they surpass the current high ids
        for ( Map.Entry<CommonAbstractStore,HighId> highId : highIds.entrySet() )
        {
            highId.getKey().setHighestPossibleIdInUse( highId.getValue().id );
        }
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    private void track( CommonAbstractStore store, long id )
    {
        HighId highId = highIds.get( store );
        if ( highId == null )
        {
            highIds.put( store, highId = new HighId( id ) );
        }
        else
        {
            highId.track( id );
        }
    }

    private void track( CommonAbstractStore store, Command command )
    {
        track( store, command.getKey() );
    }

    private void track( CommonAbstractStore store, Collection<? extends Abstract64BitRecord> records )
    {
        for ( Abstract64BitRecord record : records )
        {
            track( store, record.getId() );
        }
    }

    private <RECORD extends TokenRecord> void trackToken( TokenStore<RECORD> tokenStore,
                                                          TokenCommand<RECORD> tokenCommand )
    {
        track( tokenStore, tokenCommand );
        track( tokenStore.getNameStore(), tokenCommand.getRecord().getNameRecords() );
    }

    private static class HighId
    {
        private long id;

        public HighId( long id )
        {
            this.id = id;
        }

        void track( long id )
        {
            if ( id > this.id )
            {
                this.id = id;
            }
        }
    }
}
