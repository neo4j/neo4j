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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.index.impl.lucene.CommitContext.DocumentContext;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;

public class LuceneCommandApplier extends NeoCommandHandler.Adapter
{
    private final LuceneDataSource dataSource;
    private final Map<Byte, CommitContext> nodeContexts = new HashMap<>();
    private final Map<Byte, CommitContext> relationshipContexts = new HashMap<>();
    private IndexDefineCommand definitions;
    // TODO 2.2-future dataSource.writeLock around a commit? The old thingie did that

    public LuceneCommandApplier( LuceneDataSource dataSource )
    {
        this.dataSource = dataSource;
    }

    @Override
    public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
    {
        CommitContext context = commitContext( command );
        String key = definitions.getKey( command.getKeyId() );
        Object value = command.getValue();
        context.ensureWriterInstantiated();
        context.indexType.addToDocument( context.getDocument( command.getEntityId(), true ).document, key, value );
        context.dataSource.invalidateCache( context.identifier, key, value );
        return true;
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
    {
        CommitContext context = commitContext( command );
        String key = definitions.getKey( command.getKeyId() );
        Object value = command.getValue();
        context.ensureWriterInstantiated();
        context.indexType.addToDocument( context.getDocument( command.getEntityId(), true ).document, key, value );
        context.dataSource.invalidateCache( context.identifier, key, value );
        return true;
    }

    @Override
    public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
    {
        CommitContext context = commitContext( command );
        String key = definitions.getKey( command.getKeyId() );
        Object value = command.getValue();
        context.ensureWriterInstantiated();
        DocumentContext document = context.getDocument( command.getEntityId(), false );
        if ( document != null ) // TODO 2.2-future why null ckeck?
        {
            context.indexType.removeFromDocument( document.document, key, value );
            context.dataSource.invalidateCache( context.identifier, key, value );
        }
        return true;
    }

    @Override
    public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
    {
        CommitContext context = commitContext( command );
        context.documents.clear();
        context.dataSource.deleteIndex( context.identifier, context.recovery );
        return true;
    }

    @Override
    public boolean visitIndexCreateCommand( CreateCommand createCommand ) throws IOException
    {
        // TODO 2.2-future Indexes are created lazily, they always have been. We could create them here instead
        // but that can be changed later.
        return true;
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand indexDefineCommand ) throws IOException
    {
        definitions = indexDefineCommand;
        return true;
    }

    @Override
    public void close()
    {
        try
        {
            for ( CommitContext context : nodeContexts.values() )
            {
                context.close();
            }
            for ( CommitContext context : relationshipContexts.values() )
            {
                context.close();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failure to commit changes to lucene", e );
        }
    }

    private CommitContext commitContext( IndexCommand command )
    {
        Map<Byte, CommitContext> contextMap = commitContextMap( command.getEntityType() );
        byte indexNameId = command.getIndexNameId();
        CommitContext context = contextMap.get( indexNameId );
        if ( context == null )
        {
            IndexIdentifier identifier = new IndexIdentifier( IndexEntityType.byId( command.getEntityType() ),
                     definitions.getIndexName( indexNameId ) );

            // TODO 2.2-future the fact that we look up index type from config here using the index store
            // directly should be avoided. But how can we do it in, say recovery?
            context = new CommitContext( dataSource, identifier,
                    dataSource.getType( identifier, false ), false );   // TODO 2.2-future recovery=false
            contextMap.put( indexNameId, context );
        }
        return context;
    }

    private Map<Byte, CommitContext> commitContextMap( byte entityType )
    {
        if ( entityType == IndexEntityType.node.id() )
        {
            return nodeContexts;
        }
        if ( entityType == IndexEntityType.relationship.id() )
        {
            return relationshipContexts;
        }
        throw new IllegalArgumentException( "Unknown entity type " + entityType );
    }
}
