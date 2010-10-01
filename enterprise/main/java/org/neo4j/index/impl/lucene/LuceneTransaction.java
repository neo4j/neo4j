/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.impl.lucene.LuceneCommand.AddCommand;
import org.neo4j.index.impl.lucene.LuceneCommand.AddRelationshipCommand;
import org.neo4j.index.impl.lucene.LuceneCommand.ClearCommand;
import org.neo4j.index.impl.lucene.LuceneCommand.CreateIndexCommand;
import org.neo4j.index.impl.lucene.LuceneCommand.RemoveCommand;
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

    LuceneTransaction( int identifier, XaLogicalLog xaLog,
        LuceneDataSource luceneDs )
    {
        super( identifier, xaLog );
        this.dataSource = luceneDs;
    }

    <T extends PropertyContainer> void add( LuceneIndex<T> index, T entity,
            String key, Object value )
    {
        value = value instanceof ValueContext ? ((ValueContext) value).getCorrectValue() : value.toString();
        TxDataBoth data = getTxData( index, true );
        insert( index, entity, key, value, data.added( true ), data.removed( false ) );
        queueCommand( index.newAddCommand( entity, key, value ) ).addCount++;
    }
    
    private Object getEntityId( PropertyContainer entity )
    {
        return entity instanceof Node ? ((Node) entity).getId() :
                RelationshipId.of( (Relationship) entity );
    }
    
    <T extends PropertyContainer> TxDataBoth getTxData( LuceneIndex<T> index,
            boolean createIfNotExists )
    {
        IndexIdentifier identifier = index.identifier;
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
        insert( index, entity, key, value, data.removed( true ), data.added( false ) );
        queueCommand( index.newRemoveCommand( entity, key, value ) ).removeCount++;
    }
    
    <T extends PropertyContainer> void clear( LuceneIndex<T> index )
    {
        TxDataBoth data = getTxData( index, true );
        TxDataHolder added = data.added( false );
        if ( added != null )
        {
            added.clear();
        }
        TxDataHolder removed = data.removed( true );
        if ( removed != null )
        {
            removed.clear();
            removed.setRemoveAll();
        }
        queueCommand( new ClearCommand( index.identifier ) );
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
        commands.add( command );
        return commands;
    }
    
    private <T extends PropertyContainer> void insert( LuceneIndex<T> index,
            T entity, String key, Object value, TxDataHolder insertInto, TxDataHolder removeFrom )
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
    
    <T extends PropertyContainer> Query getExtraRemoveQuery( LuceneIndex<T> index )
    {
        TxDataHolder removed = removedTxDataOrNull( index );
        return removed != null ? removed.getExtraQuery() : null;
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
        return ids != null ? ids : Collections.<Long>emptySet();
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
        if ( data == null )
        {
            return null;
        }
        return data.added( false );
    }
    
    private <T extends PropertyContainer> TxDataHolder removedTxDataOrNull( LuceneIndex<T> index )
    {
        TxDataBoth data = getTxData( index, false );
        if ( data == null )
        {
            return null;
        }
        return data.removed( false );
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
                boolean isRecovery = entry.getValue().isRecovery();
                IndexIdentifier identifier = entry.getKey();
                IndexType type = identifier == LuceneCommand.CreateIndexCommand.FAKE_IDENTIFIER ? null :
                        dataSource.getType( identifier );
                IndexWriter writer = null;
                IndexSearcher searcher = null;
                CommandList commandList = entry.getValue();
                Map<Long, DocumentContext> documents = new HashMap<Long, DocumentContext>();
                for ( LuceneCommand command : commandList.commands )
                {
                    if ( command instanceof CreateIndexCommand )
                    {
                        CreateIndexCommand createCommand = (CreateIndexCommand) command;
                        dataSource.indexStore.setIfNecessary( createCommand.getName(),
                                createCommand.getConfig() );
                        continue;
                    }
                    
                    if ( writer == null )
                    {
                        if ( isRecovery )
                        {
                            writer = dataSource.getRecoveryIndexWriter( identifier );
                        }
                        else
                        {
                            writer = dataSource.getIndexWriter( identifier );
                            writer.setMaxBufferedDocs( commandList.addCount + 100 );
                            writer.setMaxBufferedDeleteTerms( commandList.removeCount + 100 );
                        }
                        searcher = dataSource.getIndexSearcher( identifier ).getSearcher();
                    }
                    if ( command instanceof ClearCommand )
                    {
                        documents.clear();
                        dataSource.closeWriter( writer );
                        writer = null;
                        dataSource.deleteIndex( identifier );
                        dataSource.invalidateCache( identifier );
                        if ( isRecovery )
                        {
                            dataSource.removeRecoveryIndexWriter( identifier );
                        }
                        continue;
                    }
                    
                    Object entityId = command.entityId;
                    long id = entityId instanceof Long ? (Long) entityId : ((RelationshipId)entityId).id;
                    DocumentContext context = documents.get( id );
                    if ( context == null )
                    {
                        Document document = LuceneDataSource.findDocument( type, searcher, id );
                        context = document == null ?
                                new DocumentContext( identifier.entityType.newDocument( entityId ) /*type.newDocument( entityId )*/, false, id ) :
                                new DocumentContext( document, true, id );
                        documents.put( id, context );
                    }
                    String key = command.key;
                    Object value = command.value;
                    if ( command instanceof AddCommand ||
                            command instanceof AddRelationshipCommand )
                    {
                        type.addToDocument( context.document, key, value );
                        dataSource.invalidateCache( identifier, key, value );
                    }
                    else if ( command instanceof RemoveCommand )
                    {
                        type.removeFromDocument( context.document, key, value );
                        dataSource.invalidateCache( identifier, key, value );
                    }
                    else
                    {
                        throw new RuntimeException( "Unknown command type " +
                            command + ", " + command.getClass() );
                    }
                }
                
                applyDocuments( writer, type, documents );
                if ( writer != null && !isRecovery )
                {
                    dataSource.closeWriter( writer );
                    dataSource.invalidateIndexSearcher( identifier );
                }
            }
            
            // TODO Set last committed txId
//            dataSource.setLastCommittedTxId( getCommitTxId() );
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
        for ( CommandList list : commandMap.values() )
        {
            for ( LuceneCommand command : list.commands )
            {
                addCommand( command );
            }
        }
    }

    @Override
    protected void doRollback()
    {
        // TODO Auto-generated method stub
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
        private final LuceneIndex index;
        
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
    
    private static class CommandList
    {
        private final List<LuceneCommand> commands = new ArrayList<LuceneCommand>();
        private int addCount;
        private int removeCount;
        
        void add( LuceneCommand command )
        {
            this.commands.add( command );
        }
        
        void incCounter( LuceneCommand command )
        {
            if ( command instanceof AddCommand )
            {
                this.addCount++;
            }
            else if ( command instanceof RemoveCommand )
            {
                this.removeCount++;
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
    
    private static class DocumentContext
    {
        private final Document document;
        private final boolean exists;
        private final long entityId;

        DocumentContext( Document document, boolean exists, long entityId )
        {
            this.document = document;
            this.exists = exists;
            this.entityId = entityId;
        }
    }

    <T extends PropertyContainer> boolean isRemoveAll( LuceneIndex<T> index )
    {
        TxDataHolder removed = removedTxDataOrNull( index );
        return removed != null ? removed.isRemoveAll() : false;
    }

    void createIndex( String name, Map<String, String> config )
    {
        queueCommand( new CreateIndexCommand( name, config ) );
    }
}
