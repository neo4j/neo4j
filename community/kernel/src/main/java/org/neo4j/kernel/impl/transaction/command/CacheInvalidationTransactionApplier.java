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

import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NeoStoreCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;

public class CacheInvalidationTransactionApplier extends NeoCommandHandler.Adapter
{
    private final RelationshipStore relationshipStore;
    private final NeoCommandHandler delegate;
    private final CacheAccessBackDoor cacheAccess;
    private final RelationshipTypeTokenStore relationshipTypeTokenStore;
    private final LabelTokenStore labelTokenStore;
    private final PropertyKeyTokenStore propertyKeyTokenStore;

    public CacheInvalidationTransactionApplier( NeoCommandHandler delegate, NeoStore neoStore,
                                                CacheAccessBackDoor cacheAccess )
    {
        this.delegate = delegate;
        this.cacheAccess = cacheAccess;
        this.relationshipStore = neoStore.getRelationshipStore();
        this.relationshipTypeTokenStore = neoStore.getRelationshipTypeTokenStore();
        this.labelTokenStore = neoStore.getLabelTokenStore();
        this.propertyKeyTokenStore = neoStore.getPropertyKeyTokenStore();
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command ) throws IOException
    {
        delegate.visitNodeCommand( command );

        cacheAccess.removeNodeFromCache( command.getKey() );

        return false;
    }

    @Override
    public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
    {
        delegate.visitRelationshipCommand( command );
        cacheAccess.removeRelationshipFromCache( command.getKey() );

        RelationshipRecord record = command.getRecord();
        RelationshipRecord before = null;
        if ( !record.inUse() )
        {
            /*
             * If read from a log (either on recovery or HA) then all the fields but for the Id are -1. If the
             * record is deleted, then we'll need to invalidate the cache and patch the node's relationship chains.
             * Therefore, we need to read the record from the store. This is not too expensive, since the window
             * will be either in memory or will soon be anyway and we are just saving the write the trouble.
             */
            before = relationshipStore.forceGetRaw( record.getId() );
        }

        if ( !record.inUse() )
        {    // the relationship was deleted - invalidate the cached versions of the related nodes
            if ( before != null )
            {    // reading from the log
                cacheAccess.removeNodeFromCache( before.getFirstNode() );
                cacheAccess.removeNodeFromCache( before.getSecondNode() );
            }
            else
            {    // applying from in-memory transaction state
                cacheAccess.removeNodeFromCache( record.getFirstNode() );
                cacheAccess.removeNodeFromCache( record.getSecondNode() );
            }
        }

        return false;
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
    {
        delegate.visitPropertyCommand( command );

        long nodeId = command.getNodeId();
        long relId = command.getRelId();
        if ( nodeId != -1 )
        {
            cacheAccess.removeNodeFromCache( nodeId );
        }
        else if ( relId != -1 )
        {
            cacheAccess.removeRelationshipFromCache( relId );
        }

        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException
    {
        return delegate.visitRelationshipGroupCommand( command );
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException
    {
        delegate.visitRelationshipTypeTokenCommand( command );

        Token type = relationshipTypeTokenStore.getToken( (int) command.getKey() );
        cacheAccess.addRelationshipTypeToken( type );

        return false;
    }

    @Override
    public boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException
    {
        delegate.visitLabelTokenCommand( command );

        Token labelId = labelTokenStore.getToken( (int) command.getKey() );
        cacheAccess.addLabelToken( labelId );

        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException
    {
        delegate.visitPropertyKeyTokenCommand( command );

        Token index = propertyKeyTokenStore.getToken( (int) command.getKey() );
        cacheAccess.addPropertyKeyToken( index );

        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
    {
        return delegate.visitSchemaRuleCommand( command );
    }

    @Override
    public boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException
    {
        delegate.visitNeoStoreCommand( command );

        cacheAccess.removeGraphPropertiesFromCache();

        return false;
    }

    @Override
    public void apply()
    {
        delegate.apply();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
