/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.impl.IndexStore;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.kernel.impl.util.ArrayMap;

/**
 * An {@link XaDataSource} optimized for the {@link LuceneIndexProvider}.
 * This class is public because the XA framework requires it.
 */
public class LuceneDataSource extends LogBackedXaDataSource
{
    public static final String DEFAULT_NAME = "lucene-index";
    public static final byte[] DEFAULT_BRANCH_ID = "162374".getBytes();
    
    /**
     * Default {@link Analyzer} for fulltext parsing.
     */
    public static final Analyzer LOWER_CASE_WHITESPACE_ANALYZER =
        new Analyzer()
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
        }
        
        @Override
        public String toString()
        {
            return "LOWER_CASE_WHITESPACE_ANALYZER";
        }
    };

    public static final Analyzer WHITESPACE_ANALYZER = new Analyzer()
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new WhitespaceTokenizer( reader );
        }

        @Override
        public String toString()
        {
            return "WHITESPACE_ANALYZER";
        }
    };
    
    public static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();
    
    private final Map<IndexIdentifier, IndexWriter> recoveryWriters =
        new HashMap<IndexIdentifier, IndexWriter>();
    private final ArrayMap<IndexIdentifier,IndexSearcherRef> indexSearchers = 
        new ArrayMap<IndexIdentifier,IndexSearcherRef>( 6, true, true );

    private final XaContainer xaContainer;
    private final String baseStorePath;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); 
    final IndexStore indexStore;
    final LuceneIndexStore store;
    private final IndexTypeCache typeCache;
    private boolean closed;
    private final Cache caching;
    EntityType nodeEntityType;
    EntityType relationshipEntityType;

    /**
     * Constructs this data source.
     * 
     * @param params XA parameters.
     * @throws InstantiationException if the data source couldn't be
     * instantiated
     */
    public LuceneDataSource( Map<Object,Object> params ) 
        throws InstantiationException
    {
        super( params );
        caching = new Cache();
        String storeDir = (String) params.get( "store_dir" );
        this.baseStorePath = getStoreDir( storeDir );
        cleanWriteLocks( baseStorePath );
        this.indexStore = new IndexStore( storeDir );
        this.store = newIndexStore( storeDir );
        this.typeCache = new IndexTypeCache();
        boolean isReadOnly = params.containsKey( "read_only" ) ?
                (Boolean) params.get( "read_only" ) : false;
                
        nodeEntityType = new EntityType()
        {
            public Document newDocument( Object entityId )
            {
                return IndexType.newBaseDocument( (Long) entityId );
            }
            
            public Class<?> getType()
            {
                return Node.class;
            }
        };
        relationshipEntityType = new EntityType()
        {
            public Document newDocument( Object entityId )
            {
                RelationshipId relId = (RelationshipId) entityId;
                Document doc = IndexType.newBaseDocument( relId.id );
                doc.add( new Field( LuceneIndex.KEY_START_NODE_ID, "" + relId.startNode,
                        Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
                doc.add( new Field( LuceneIndex.KEY_END_NODE_ID, "" + relId.endNode,
                        Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
                return doc;
            }

            public Class<?> getType()
            {
                return Relationship.class;
            }
        };
                
        if ( !isReadOnly )
        {
            XaCommandFactory cf = new LuceneCommandFactory();
            XaTransactionFactory tf = new LuceneTransactionFactory( store );
            xaContainer = XaContainer.create( this.baseStorePath + "/lucene.log", cf,
                    tf, params );
            try
            {
                xaContainer.openLogicalLog();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to open lucene log in " +
                        this.baseStorePath, e );
            }
            
            xaContainer.getLogicalLog().setKeepLogs(
                    shouldKeepLog( (String) params.get( Config.KEEP_LOGICAL_LOGS ), DEFAULT_NAME ) );
            setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );
        }
        else
        {
            xaContainer = null;
        }
    }
    
    IndexType getType( IndexIdentifier identifier )
    {
        return typeCache.getIndexType( identifier );
    }
    
    private void cleanWriteLocks( String directory )
    {
        File dir = new File( directory );
        if ( !dir.isDirectory() )
        {
            return;
        }
        for ( File file : dir.listFiles() )
        {
            if ( file.isDirectory() )
            {
                cleanWriteLocks( file.getAbsolutePath() );
            }
            else if ( file.getName().equals( "write.lock" ) )
            {
                boolean success = file.delete();
                assert success;
            }
        }
    }
    
    static String getStoreDir( String dbStoreDir )
    {
        File dir = new File( new File( dbStoreDir ), "index" );
        if ( !dir.exists() )
        {
            if ( !dir.mkdirs() )
            {
                throw new RuntimeException( "Unable to create directory path["
                    + dir.getAbsolutePath() + "] for Neo4j store." );
            }
        }
        return dir.getAbsolutePath();
    }
    
    static LuceneIndexStore newIndexStore( String dbStoreDir )
    {
        return new LuceneIndexStore( getStoreDir( dbStoreDir ) + "/lucene-store.db" );
    }

    @Override
    public void close()
    {
        if ( closed )
        {
            return;
        }
        
        for ( IndexSearcherRef searcher : indexSearchers.values() )
        {
            try
            {
                searcher.dispose();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        indexSearchers.clear();
        if ( xaContainer != null )
        {
            xaContainer.close();
        }
        store.close();
        closed = true;
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new LuceneXaConnection( baseStorePath, xaContainer
            .getResourceManager(), getBranchId() );
    }
    
    private class LuceneCommandFactory extends XaCommandFactory
    {
        LuceneCommandFactory()
        {
            super();
        }

        @Override
        public XaCommand readCommand( ReadableByteChannel channel, 
            ByteBuffer buffer ) throws IOException
        {
            return LuceneCommand.readCommand( channel, buffer, LuceneDataSource.this );
        }
    }
    
    private class LuceneTransactionFactory extends XaTransactionFactory
    {
        private final LuceneIndexStore store;
        
        LuceneTransactionFactory( LuceneIndexStore store )
        {
            this.store = store;
        }
        
        @Override
        public XaTransaction create( int identifier )
        {
            return createTransaction( identifier, this.getLogicalLog() );
        }

        @Override
        public void flushAll()
        {
            // Not much we can do...
        }

        @Override
        public long getCurrentVersion()
        {
            return store.getVersion();
        }
        
        @Override
        public long getAndSetNewVersion()
        {
            return store.incrementVersion();
        }
        
        @Override
        public void recoveryComplete()
        {
            for ( Map.Entry<IndexIdentifier, IndexWriter> entry : recoveryWriters.entrySet() )
            {
                closeWriter( entry.getValue() );
                invalidateIndexSearcher( entry.getKey() );
            }
            recoveryWriters.clear();
        }        
//
//        @Override
//        public long getLastCommittedTx()
//        {
//            return store.getLastCommittedTx();
//        }
    }
    
    void getReadLock()
    {
        lock.readLock().lock();
    }
    
    void releaseReadLock()
    {
        lock.readLock().unlock();
    }
    
    void getWriteLock()
    {
        lock.writeLock().lock();
    }
    
    void releaseWriteLock()
    {
        lock.writeLock().unlock();
    }
    
    /**
     * If nothing has changed underneath (since the searcher was last created
     * or refreshed) {@code null} is returned. But if something has changed a
     * refreshed searcher is returned. It makes use if the
     * {@link IndexReader#reopen()} which faster than opening an index from
     * scratch.
     * 
     * @param searcher the {@link IndexSearcher} to refresh.
     * @return a refreshed version of the searcher or, if nothing has changed,
     * {@code null}.
     * @throws IOException if there's a problem with the index.
     */
    private IndexSearcherRef refreshSearcher( IndexSearcherRef searcher )
    {
        try
        {
            IndexReader reader = searcher.getSearcher().getIndexReader();
            IndexReader reopened = reader.reopen();
            if ( reopened != reader )
            {
                IndexSearcher newSearcher = new IndexSearcher( reopened );
                searcher.detachOrClose();
                return new IndexSearcherRef( searcher.getIdentifier(), newSearcher );
            }
            return null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    static File getFileDirectory( String storeDir, IndexIdentifier identifier )
    {
        File path = new File( storeDir, "lucene" );
        String extra = null;
        if ( identifier.entityTypeByte == LuceneCommand.NODE )
        {
            extra = "node";
        }
        else if ( identifier.entityTypeByte == LuceneCommand.RELATIONSHIP )
        {
            extra = "relationship";
        }
        else
        {
            throw new RuntimeException( identifier.entityType.getType().getName() );
        }
        return new File( path, extra );
    }
    
    static Directory getDirectory( String storeDir,
            IndexIdentifier identifier ) throws IOException
    {
        return FSDirectory.open( new File( getFileDirectory( storeDir, identifier),
                identifier.indexName ) );
    }
    
    IndexSearcherRef getIndexSearcher( IndexIdentifier identifier )
    {
        try
        {
            IndexSearcherRef searcher = indexSearchers.get( identifier );
            if ( searcher == null )
            {
                Directory dir = getDirectory( baseStorePath, identifier );
                try
                {
                    String[] files = dir.listAll();
                    if ( files == null || files.length == 0 )
                    {
                        return null;
                    }
                }
                catch ( IOException e )
                {
                    return null;
                }
                IndexReader indexReader = IndexReader.open( dir, false );
                IndexSearcher indexSearcher = new IndexSearcher( indexReader );
                searcher = new IndexSearcherRef( identifier, indexSearcher );
                indexSearchers.put( identifier, searcher );
            }
            return searcher;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog )
    {
        return new LuceneTransaction( identifier, logicalLog, this );
    }

    void invalidateIndexSearcher( IndexIdentifier identifier )
    {
        IndexSearcherRef searcher = indexSearchers.get( identifier );
        if ( searcher != null )
        {
            IndexSearcherRef refreshedSearcher = refreshSearcher( searcher );
            if ( refreshedSearcher != null )
            {
                indexSearchers.put( identifier, refreshedSearcher );
            }
        }
    }

    void closeIndexSearcher( IndexIdentifier identifier )
    {
        try
        {
            IndexSearcherRef searcher = indexSearchers.remove( identifier );
            if ( searcher != null )
            {
                searcher.dispose();
            }
        }
        catch ( IOException e )
        { // OK
        }
    }
    
    void deleteIndex( IndexIdentifier identifier )
    {
        closeIndexSearcher( identifier );
        deleteFileOrDirectory( getFileDirectory( baseStorePath, identifier ) );
    }
    
    private static void deleteFileOrDirectory( File file )
    {
        if ( file.exists() )
        {
            if ( file.isDirectory() )
            {
                for ( File child : file.listFiles() )
                {
                    deleteFileOrDirectory( child );
                }
            }
            file.delete();
        }
    }
    
    synchronized IndexWriter getIndexWriter( IndexIdentifier identifier )
    {
        try
        {
            Directory dir = getDirectory( baseStorePath, identifier );
            directoryExists( dir );
            IndexType type = getType( identifier );
            IndexWriter writer = new IndexWriter( dir, type.analyzer, MaxFieldLength.UNLIMITED );
            
            // TODO We should tamper with this value and see how it affects the
            // general performance. Lucene docs says rather <10 for mixed
            // reads/writes 
//            writer.setMergeFactor( 8 );
            
            return writer;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    synchronized IndexWriter getRecoveryIndexWriter( IndexIdentifier identifier )
    {
        IndexWriter writer = recoveryWriters.get( identifier );
        if ( writer == null )
        {
            writer = getIndexWriter( identifier );
            recoveryWriters.put( identifier, writer );
        }
        return writer;
    }
    
    synchronized void removeRecoveryIndexWriter( IndexIdentifier identifier )
    {
        recoveryWriters.remove( identifier );
    }
    
    private boolean directoryExists( Directory dir )
    {
        try
        {
            String[] files = dir.listAll();
            return files != null && files.length > 0;
        }
        catch ( IOException e )
        {
            return false;
        }
    }
    
    static Document findDocument( IndexType type, IndexSearcher searcher, long entityId )
    {
        try
        {
            TopDocs docs = searcher.search( type.idTermQuery( entityId ), 1 );
            if ( docs.scoreDocs.length > 0 )
            {
                return searcher.doc( docs.scoreDocs[0].doc );
            }
            return null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    static boolean documentIsEmpty( Document document )
    {
        List<Fieldable> fields = document.getFields();
        for ( Fieldable field : fields )
        {
            if ( !LuceneIndex.KEY_DOC_ID.equals( field.name() ) )
            {
                return false;
            }
        }
        return true;
    }

    static void remove( IndexWriter writer, Query query )
    {
        try
        {
            // TODO
            writer.deleteDocuments( query );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to delete for " + query + " using" + writer, e );
        }
    }
    
    void closeWriter( IndexWriter writer )
    {
        try
        {
            writer.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to close lucene writer "
                + writer, e );
        }
    }

    LruCache<String,Collection<Long>> getFromCache( IndexIdentifier identifier,
            String key )
    {
        return caching.get( identifier, key );
    }

    void setCacheCapacity( IndexIdentifier identifier, String key, int maxNumberOfCachedEntries )
    {
        this.caching.setCapacity( identifier, key, maxNumberOfCachedEntries );
    }
    
    Integer getCacheCapacity( IndexIdentifier identifier, String key )
    {
        LruCache<String, Collection<Long>> cache = this.caching.get( identifier, key );
        return cache != null ? cache.maxSize() : null;
    }
    
    void invalidateCache( IndexIdentifier identifier, String key, Object value )
    {
        LruCache<String,Collection<Long>> cache = caching.get( identifier, key );
        if ( cache != null )
        {
            cache.remove( value.toString() );
        }
    }
    
    void invalidateCache( IndexIdentifier identifier )
    {
        this.caching.disable( identifier );
    }
    
    @Override
    public long getCreationTime()
    {
        return store.getCreationTime();
    }
    
    @Override
    public long getRandomIdentifier()
    {
        return store.getRandomNumber();
    }
    
    @Override
    public long getCurrentLogVersion()
    {
        return store.getVersion();
    }
    
//    @Override
//    public long getLastCommittedTxId()
//    {
//        return store.getLastCommittedTx();
//    }

    public void setLastCommittedTxId( long txId )
    {
        store.setLastCommittedTx( txId );
    }
    
//    @Override
//    public XaContainer getXaContainer()
//    {
//        return this.xaContainer;
//    }
}
