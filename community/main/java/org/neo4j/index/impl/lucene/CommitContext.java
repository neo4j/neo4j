package org.neo4j.index.impl.lucene;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.neo4j.index.impl.lucene.LuceneTransaction.CommandList;

/**
 * This presents a context for each {@link LuceneCommand} when they are
 * committing its data.
 */
class CommitContext
{
    final LuceneDataSource dataSource;
    final IndexIdentifier identifier;
    final IndexType indexType;
    final boolean isRecovery;
    final Map<Long, DocumentContext> documents = new HashMap<Long, DocumentContext>();
    final CommandList commandList;
    
    IndexWriter writer;
    IndexSearcher searcher;
    
    CommitContext( LuceneDataSource dataSource, IndexIdentifier identifier, IndexType indexType, CommandList commandList )
    {
        this.dataSource = dataSource;
        this.identifier = identifier;
        this.indexType = indexType;
        this.commandList = commandList;
        
        // TODO There's an issue with recovery mode when you apply individual transactions
        // so this is disabled a.t.m. an enabled recovery mode would yield much higher
        // recovery performance.
        this.isRecovery = false; // commandList.isRecovery();
    }
    
    void safeCloseWriter()
    {
        if ( this.writer != null )
        {
            LuceneDataSource.closeWriter( this.writer );
            this.writer = null;
        }
    }
    
    void ensureWriterInstantiated()
    {
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
    }
    
    DocumentContext getDocument( Object entityId )
    {
        long id = entityId instanceof Long ? (Long) entityId : ((RelationshipId)entityId).id;
        DocumentContext context = documents.get( id );
        if ( context == null )
        {
            Document document = LuceneDataSource.findDocument( indexType, searcher, id );
            context = document == null ?
                    new DocumentContext( identifier.entityType.newDocument( entityId ), false, id ) :
                    new DocumentContext( document, true, id );
            documents.put( id, context );
        }
        return context;
    }

    static class DocumentContext
    {
        final Document document;
        final boolean exists;
        final long entityId;

        DocumentContext( Document document, boolean exists, long entityId )
        {
            this.document = document;
            this.exists = exists;
            this.entityId = entityId;
        }
    }
}
