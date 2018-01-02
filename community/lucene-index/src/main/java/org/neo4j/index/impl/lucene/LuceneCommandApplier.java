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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.index.impl.lucene.CommitContext.DocumentContext;
import org.neo4j.index.impl.lucene.EntityId.IdData;
import org.neo4j.index.impl.lucene.EntityId.RelationshipData;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;

/**
 * Applies changes from {@link IndexCommand commands} onto one ore more indexes from the same
 * {@link IndexImplementation provider}.
 */
public class LuceneCommandApplier extends CommandHandler.Adapter
{
    private final LuceneDataSource dataSource;
    private final Map<String,CommitContext> nodeContexts = new HashMap<>();
    private final Map<String,CommitContext> relationshipContexts = new HashMap<>();
    private final boolean recovery;
    private IndexDefineCommand definitions;

    public LuceneCommandApplier( LuceneDataSource dataSource, boolean recovery )
    {
        this.dataSource = dataSource;
        this.recovery = recovery;
    }

    @Override
    public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
    {
        return visitIndexAddCommand( command, new IdData( command.getEntityId() ) );
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
    {
        RelationshipData entityId = new RelationshipData( command.getEntityId(),
                command.getStartNode(), command.getEndNode() );
        return visitIndexAddCommand( command, entityId );
    }

    private boolean visitIndexAddCommand( IndexCommand command, EntityId entityId )
    {
        CommitContext context = commitContext( command );
        String key = definitions.getKey( command.getKeyId() );
        Object value = command.getValue();

        // Below is a check for a null value where such a value is ignored. This may look strange, but the
        // reason is that there was this bug where adding a null value to an index would be fine and written
        // into the log as a command, to later fail during application of that command, i.e. here.
        // There was a fix introduced to throw IllegalArgumentException out to user right away if passing in
        // null or object that had toString() produce null. Although databases already affected by this would
        // not be able to recover, which is why this check is here.
        if ( value != null )
        {
            context.ensureWriterInstantiated();
            context.indexType.addToDocument( context.getDocument( entityId, true ).document, key, value );
        }
        return false;
    }

    @Override
    public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
    {
        CommitContext context = commitContext( command );
        String key = definitions.getKey( command.getKeyId() );
        Object value = command.getValue();
        context.ensureWriterInstantiated();
        DocumentContext document = context.getDocument( new IdData( command.getEntityId() ), false );
        if ( document != null )
        {
            context.indexType.removeFromDocument( document.document, key, value );
        }
        return false;
    }

    @Override
    public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
    {
        CommitContext context = commitContext( command );
        context.documents.clear();
        context.dataSource.deleteIndex( context.identifier, context.recovery );
        return false;
    }

    @Override
    public boolean visitIndexCreateCommand( CreateCommand createCommand ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand indexDefineCommand ) throws IOException
    {
        definitions = indexDefineCommand;
        return false;
    }

    @Override
    public void apply()
    {
        try
        {
            if ( definitions != null )
            {
                dataSource.getWriteLock();
                for ( CommitContext context : nodeContexts.values() )
                {
                    context.close();
                }
                for ( CommitContext context : relationshipContexts.values() )
                {
                    context.close();
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failure to commit changes to lucene", e );
        }
    }

    @Override
    public void close()
    {
        if ( definitions != null )
        {
            dataSource.releaseWriteLock();
        }
    }

    private CommitContext commitContext( IndexCommand command )
    {
        Map<String,CommitContext> contextMap = commitContextMap( command.getEntityType() );
        String indexName = definitions.getIndexName( command.getIndexNameId() );
        CommitContext context = contextMap.get( indexName );
        if ( context == null )
        {
            IndexIdentifier identifier =
                    new IndexIdentifier( IndexEntityType.byId( command.getEntityType() ), indexName );

            // TODO the fact that we look up index type from config here using the index store
            // directly should be avoided. But how can we do it in, say recovery?
            context = new CommitContext( dataSource, identifier,
                    dataSource.getType( identifier, recovery ), recovery );
            contextMap.put( indexName, context );
        }
        return context;
    }

    private Map<String,CommitContext> commitContextMap( byte entityType )
    {
        if ( entityType == IndexEntityType.Node.id() )
        {
            return nodeContexts;
        }
        if ( entityType == IndexEntityType.Relationship.id() )
        {
            return relationshipContexts;
        }
        throw new IllegalArgumentException( "Unknown entity type " + entityType );
    }
}
