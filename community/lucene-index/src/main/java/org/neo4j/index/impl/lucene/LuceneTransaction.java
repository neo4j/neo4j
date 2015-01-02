/**
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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.impl.lucene.CommitContext.DocumentContext;
import org.neo4j.index.impl.lucene.LuceneCommand.CreateIndexCommand;
import org.neo4j.index.impl.lucene.LuceneCommand.DeleteCommand;
import org.neo4j.index.impl.lucene.LuceneCommand.RemoveCommand;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;

class LuceneTransaction extends XaTransaction
{
    private final Map<IndexIdentifier, TxDataBoth> txData =
            new HashMap<IndexIdentifier, TxDataBoth>();
    private final LuceneDataSource dataSource;

    private final Map<IndexIdentifier,CommandList> commandMap =
            new HashMap<IndexIdentifier,CommandList>();

    LuceneTransaction( XaLogicalLog xaLog, TransactionState state,
        LuceneDataSource luceneDs )
    {
        super( xaLog, state );
        this.dataSource = luceneDs;
    }

    <T extends PropertyContainer> void add( LuceneIndex<T> index, T entity,
            String key, Object value )
    {
        value = value instanceof ValueContext ? ((ValueContext) value).getCorrectValue() : value.toString();
        TxDataBoth data = getTxData( index, true );
        insert( entity, key, value, data.added( true ), data.removed( false ) );
        queueCommand( index.newAddCommand( entity, key, value ) );
    }

    private Object getEntityId( PropertyContainer entity )
    {
        return entity instanceof Node ? ((Node) entity).getId() :
                RelationshipId.of( (Relationship) entity );
    }

    <T extends PropertyContainer> TxDataBoth getTxData( LuceneIndex<T> index,
            boolean createIfNotExists )
    {
        IndexIdentifier identifier = index.getIdentifier();
        TxDataBoth data = txData.get( identifier );
        if ( data == null && createIfNotExists )
        {
            data = new TxDataBoth( index );
            txData.put( identifier, data );
        }
        return data;
    }

    <T extends PropertyContainer> void remove( LuceneIndex<T> index, T entity,
            String key, Object value )
    {
        value = value instanceof ValueContext ? ((ValueContext) value).getCorrectValue() : value.toString();
        TxDataBoth data = getTxData( index, true );
        insert( entity, key, value, data.removed( true ), data.added( false ) );
        queueCommand( index.newRemoveCommand( entity, key, value ) );
    }

    <T extends PropertyContainer> void remove( LuceneIndex<T> index, T entity, String key )
    {
        TxDataBoth data = getTxData( index, true );
        insert( entity, key, null, data.removed( true ), data.added( false ) );
        queueCommand( index.newRemoveCommand( entity, key, null ) );
    }

    <T extends PropertyContainer> void remove( LuceneIndex<T> index, T entity )
    {
        TxDataBoth data = getTxData( index, true );
        insert( entity, null, null, data.removed( true ), data.added( false ) );
        queueCommand( index.newRemoveCommand( entity, null, null ) );
    }

    <T extends PropertyContainer> void delete( LuceneIndex<T> index )
    {
        txData.put( index.getIdentifier(), new DeletedTxDataBoth( index ) );
        queueCommand( new DeleteCommand( index.getIdentifier() ) );
    }

    private CommandList queueCommand( LuceneCommand command )
    {
        IndexIdentifier indexId = command.indexId;
        CommandList commands = commandMap.get( indexId );
        if ( commands == null )
        {
            commands = new CommandList();
            commandMap.put( indexId, commands );
        }
        if ( command instanceof DeleteCommand )
        {
            commands.clear();
        }
        commands.add( command );
        commands.incCounter( command );
        return commands;
    }

    private <T extends PropertyContainer> void insert( T entity, String key, Object value, TxDataHolder insertInto,
                                                       TxDataHolder removeFrom )
    {
        Object id = getEntityId( entity );
        if ( removeFrom != null )
        {
            removeFrom.remove( id, key, value );
        }
        insertInto.add( id, key, value );
    }

    <T extends PropertyContainer> Collection<Long> getRemovedIds( LuceneIndex<T> index, Query query )
    {
        TxDataHolder removed = removedTxDataOrNull( index );
        if ( removed == null )
        {
            return Collections.emptySet();
        }
        Collection<Long> ids = removed.query( query, null );
        return ids != null ? ids : Collections.<Long>emptySet();
    }

    <T extends PropertyContainer> Collection<Long> getRemovedIds( LuceneIndex<T> index,
            String key, Object value )
    {
        TxDataHolder removed = removedTxDataOrNull( index );
        if ( removed == null )
        {
            return Collections.emptySet();
        }
        Collection<Long> ids = removed.get( key, value );
        Collection<Long> orphanIds = removed.getOrphans( key );
        return merge( ids, orphanIds );
    }

    static Collection<Long> merge( Collection<Long> c1, Collection<Long> c2 )
    {
        if ( c1 == null && c2 == null )
        {
            return Collections.<Long>emptySet();
        }
        else if ( c1 != null && c2 != null )
        {
            if (c1.isEmpty())
            {
                return c2;
            }
            if (c2.isEmpty())
            {
                return c1;
            }
            Collection<Long> result = new HashSet<Long>( c1.size()+c2.size(), 1 );
            result.addAll( c1 );
            result.addAll( c2 );
            return result;
        }
        else
        {
            return c1 != null ? c1 : c2;
        }
    }

    <T extends PropertyContainer> Collection<Long> getAddedIds( LuceneIndex<T> index,
            Query query, QueryContext contextOrNull )
    {
        TxDataHolder added = addedTxDataOrNull( index );
        if ( added == null )
        {
            return Collections.emptySet();
        }
        Collection<Long> ids = added.query( query, contextOrNull );
        return ids != null ? ids : Collections.<Long>emptySet();
    }

    <T extends PropertyContainer> Collection<Long> getAddedIds( LuceneIndex<T> index,
            String key, Object value )
    {
        TxDataHolder added = addedTxDataOrNull( index );
        if ( added == null )
        {
            return Collections.emptySet();
        }
        Collection<Long> ids = added.get( key, value );
        return ids != null ? ids : Collections.<Long>emptySet();
    }

    private <T extends PropertyContainer> TxDataHolder addedTxDataOrNull( LuceneIndex<T> index )
    {
        TxDataBoth data = getTxData( index, false );
        return data != null ? data.added( false ) : null;
    }

    private <T extends PropertyContainer> TxDataHolder removedTxDataOrNull( LuceneIndex<T> index )
    {
        TxDataBoth data = getTxData( index, false );
        return data != null ? data.removed( false ) : null;
    }

    @Override
    protected void doAddCommand( XaCommand command )
    { // we override inject command and manage our own in memory command list
    }

    @Override
    protected void injectCommand( XaCommand command )
    {
        queueCommand( ( LuceneCommand ) command ).incCounter( (LuceneCommand ) command );
    }

    @Override
    protected void doCommit()
    {
        dataSource.getWriteLock();
        try
        {
            for ( Map.Entry<IndexIdentifier, CommandList> entry :
                this.commandMap.entrySet() )
            {
                if ( entry.getValue().isEmpty() )
                {
                    continue;
                }

                IndexIdentifier identifier = entry.getKey();
                CommandList commandList = entry.getValue();
                IndexType type = identifier == LuceneCommand.CreateIndexCommand.FAKE_IDENTIFIER
                                 || !commandList.containsWrites() ? null : dataSource.getType( identifier, isRecovered() );
                
                // This is for an issue where there are changes to and index which in a later
                // transaction becomes deleted and crashes before the next rotation.
                // The next recovery process will then recover that log and do those changes
                // to the index, which at this point doesn't exist. So just don't do those
                // changes as it will be deleted "later" anyway.
                if ( type == null && isRecovered() )
                {
                    if ( commandList.isDeletion() )
                    {
                        dataSource.removeExpectedFutureDeletion( identifier );
                    }
                    else if ( commandList.containsWrites() )
                    {
                        dataSource.addExpectedFutureDeletion( identifier );
                        continue;
                    }
                }
                
                CommitContext context = null;
                try
                {
                    context = new CommitContext( dataSource, identifier, type, commandList );
                    for ( LuceneCommand command : commandList.commands )
                    {
                        command.perform( context );
                    }

                    applyDocuments( context.writer, type, context.documents );
                    if ( context.writer != null )
                    {
                        dataSource.invalidateIndexSearcher( identifier );
                    }
                }
                finally
                {
                    if ( context != null )
                    {
                        context.close();
                    }
                }
            }

            dataSource.setLastCommittedTxId( getCommitTxId() );
            closeTxData();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            dataSource.releaseWriteLock();
        }
    }

    private void applyDocuments( IndexWriter writer, IndexType type,
            Map<Long, DocumentContext> documents ) throws IOException
    {
        for ( Map.Entry<Long, DocumentContext> entry : documents.entrySet() )
        {
            DocumentContext context = entry.getValue();
            if ( context.exists )
            {
                if ( LuceneDataSource.documentIsEmpty( context.document ) )
                {
                    writer.deleteDocuments( type.idTerm( context.entityId ) );
                }
                else
                {
                    writer.updateDocument( type.idTerm( context.entityId ), context.document );
                }
            }
            else
            {
                writer.addDocument( context.document );
            }
        }
    }

    private void closeTxData()
    {
        for ( TxDataBoth data : this.txData.values() )
        {
            data.close();
        }
        this.txData.clear();
    }

    @Override
    protected void doPrepare()
    {
        boolean containsDeleteCommand = false;
        for ( CommandList list : commandMap.values() )
        {
            for ( LuceneCommand command : list.commands )
            {
                if ( command instanceof DeleteCommand )
                {
                    containsDeleteCommand = true;
                }
                addCommand( command );
            }
        }
        if ( !containsDeleteCommand )
        { // unless the entire index is deleted
            addAbandonedEntitiesToTheTx();
        } // else: the DeleteCommand will clear abandonedIds
    }

    private void addAbandonedEntitiesToTheTx()
    {
        for ( Map.Entry<IndexIdentifier, TxDataBoth> entry : txData.entrySet() )
        {
            Collection<Long> abandonedIds = entry.getValue().index.abandonedIds;
            if ( !abandonedIds.isEmpty() )
            {
                CommandList commands = commandMap.get( entry.getKey() );
                for ( Long id : abandonedIds )
                {
                    RemoveCommand command = new RemoveCommand( entry.getKey(), entry.getKey().entityTypeByte, id, null, null );
                    addCommand( command );
                    commands.add( command );
                }
                abandonedIds.clear();
            }
        }
    }

    @Override
    protected void doRollback()
    {
        commandMap.clear();
        closeTxData();
    }

    @Override
    public boolean isReadOnly()
    {
        return commandMap.isEmpty();
    }

    // Bad name
    private class TxDataBoth
    {
        private TxDataHolder add;
        private TxDataHolder remove;
        final LuceneIndex index;

        public TxDataBoth( LuceneIndex index )
        {
            this.index = index;
        }

        TxDataHolder added( boolean createIfNotExists )
        {
            if ( this.add == null && createIfNotExists )
            {
                this.add = new TxDataHolder( index, index.type.newTxData( index ) );
            }
            return this.add;
        }

        TxDataHolder removed( boolean createIfNotExists )
        {
            if ( this.remove == null && createIfNotExists )
            {
                this.remove = new TxDataHolder( index, index.type.newTxData( index ) );
            }
            return this.remove;
        }

        void close()
        {
            safeClose( add );
            safeClose( remove );
        }

        private void safeClose( TxDataHolder data )
        {
            if ( data != null )
            {
                data.close();
            }
        }
    }

    private class DeletedTxDataBoth extends TxDataBoth
    {
        public DeletedTxDataBoth( LuceneIndex index )
        {
            super( index );
        }

        @Override
        TxDataHolder added( boolean createIfNotExists )
        {
            throw illegalStateException();
        }

        @Override
        TxDataHolder removed( boolean createIfNotExists )
        {
            throw illegalStateException();
        }

        private IllegalStateException illegalStateException()
        {
            throw new IllegalStateException( "This index (" + index.getIdentifier() +
                    ") has been marked as deleted in this transaction" );
        }
    }

    static class CommandList
    {
        private final List<LuceneCommand> commands = new ArrayList<LuceneCommand>();
        private boolean containsWrites;

        void add( LuceneCommand command )
        {
            this.commands.add( command );
        }

        boolean containsWrites()
        {
            return containsWrites;
        }
        
        boolean isDeletion()
        {
            return commands.size() == 1 && commands.get( 0 ) instanceof DeleteCommand;
        }

        void clear()
        {
            commands.clear();
            containsWrites = false;
        }

        void incCounter( LuceneCommand command )
        {
            if ( command.isConsideredNormalWriteCommand() )
            {
                containsWrites = true;
            }
        }

        boolean isEmpty()
        {
            return commands.isEmpty();
        }

        boolean isRecovery()
        {
            return commands.get( 0 ).isRecovered();
        }
    }

    void createIndex( Class<? extends PropertyContainer> entityType, String name,
            Map<String, String> config )
    {
        byte entityTypeByte = 0;
        if ( entityType == Node.class )
        {
            entityTypeByte = LuceneCommand.NODE;
        }
        else if ( entityType == Relationship.class )
        {
            entityTypeByte = LuceneCommand.RELATIONSHIP;
        }
        else
        {
            throw new IllegalArgumentException( "Unknown entity typee " + entityType );
        }
        queueCommand( new CreateIndexCommand( entityTypeByte, name, config ) );
    }

    <T extends PropertyContainer> IndexSearcher getAdditionsAsSearcher( LuceneIndex<T> index,
            QueryContext context )
    {
        TxDataHolder data = addedTxDataOrNull( index );
        return data != null ? data.asSearcher( context ) : null;
    }
}
