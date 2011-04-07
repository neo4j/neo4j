/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.ClosableIterable;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.index.IndexProviderStore;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;

/**
 * An {@link XaDataSource} optimized for the {@link LuceneIndexImplementation}.
 * This class is public because the XA framework requires it.
 */
public class LuceneDataSource extends LogBackedXaDataSource
{
    public static final String DEFAULT_NAME = "lucene-index";
    public static final byte[] DEFAULT_BRANCH_ID = UTF8.encode( "162374" );
    
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
    
    private final Map<IndexIdentifier,Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy>> indexWriters = 
        new HashMap<IndexIdentifier,Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy>>();
    private final Map<IndexIdentifier,IndexSearcherRef> indexSearchers = 
        new HashMap<IndexIdentifier,IndexSearcherRef>();

    private final XaContainer xaContainer;
    private final String baseStorePath;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); 
    final IndexStore indexStore;
    final IndexProviderStore providerStore;
    private final IndexTypeCache typeCache;
    private boolean closed;
    private final Cache caching;
    EntityType nodeEntityType;
    EntityType relationshipEntityType;
    final Map<IndexIdentifier, LuceneIndex<? extends PropertyContainer>> indexes =
            new HashMap<IndexIdentifier, LuceneIndex<? extends PropertyContainer>>();

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
        this.baseStorePath = getStoreDir( storeDir ).first();
        cleanWriteLocks( baseStorePath );
        this.indexStore = (IndexStore) params.get( IndexStore.class );
        this.providerStore = newIndexStore( storeDir );
        this.typeCache = new IndexTypeCache( indexStore );
        boolean isReadOnly = false;
        if ( params.containsKey( "read_only" ) )
        {
            Object readOnly = params.get( "read_only" );
            if ( readOnly instanceof Boolean )
            {
                isReadOnly = (Boolean) readOnly;
            }
            else
            {
                isReadOnly = Boolean.parseBoolean( (String) readOnly );
            }
        }
                
        nodeEntityType = new EntityType()
        {
            public Document newDocument( Object entityId )
            {
                return IndexType.newBaseDocument( (Long) entityId );
            }
            
            public Class<? extends PropertyContainer> getType()
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

            public Class<? extends PropertyContainer> getType()
            {
                return Relationship.class;
            }
        };

        XaCommandFactory cf = new LuceneCommandFactory();
        XaTransactionFactory tf = new LuceneTransactionFactory();
        xaContainer = XaContainer.create( this, this.baseStorePath + File.separator + "lucene.log", cf, tf, params );

        if ( !isReadOnly )
        {
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
    }
    
    IndexType getType( IndexIdentifier identifier )
    {
        return typeCache.getIndexType( identifier );
    }
    
    Map<String, String> getConfig( IndexIdentifier identifier )
    {
        return indexStore.get( identifier.entityType.getType(), identifier.indexName );
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
    
    static Pair<String, Boolean> getStoreDir( String dbStoreDir )
    {
        File dir = new File( new File( dbStoreDir ), "index" );
        boolean created = false;
        if ( !dir.exists() )
        {
            if ( !dir.mkdirs() )
            {
                throw new RuntimeException( "Unable to create directory path["
                    + dir.getAbsolutePath() + "] for Neo4j store." );
            }
            created = true;
        }
        return Pair.of( dir.getAbsolutePath(), created );
    }
    
    static IndexProviderStore newIndexStore( String dbStoreDir )
    {
        File file = new File( getStoreDir( dbStoreDir ).first() + File.separator + "lucene-store.db" );
        return new IndexProviderStore( file );
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
        
        for ( Map.Entry<IndexIdentifier, Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy>> entry : indexWriters.entrySet() )
        {
            try
            {
                entry.getValue().first().close( true );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to close index writer " + entry.getKey(), e );
            }
        }
        indexWriters.clear();
        
        if ( xaContainer != null )
        {
            xaContainer.close();
        }
        providerStore.close();
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
        @Override
        public XaTransaction create( int identifier )
        {
            return createTransaction( identifier, this.getLogicalLog() );
        }

        @Override
        public void flushAll()
        {
            for ( Map.Entry<IndexIdentifier, Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy>> entry : indexWriters.entrySet() )
            {
                try
                {
                    entry.getValue().first().commit();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "unable to commit changes to " + entry.getKey(), e );
                }
            }
        }

        @Override
        public long getCurrentVersion()
        {
            return providerStore.getVersion();
        }
        
        @Override
        public long getAndSetNewVersion()
        {
            return providerStore.incrementVersion();
        }

        @Override
        public long getLastCommittedTx()
        {
            return providerStore.getLastCommittedTx();
        }
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
    
    static File getFileDirectory( String storeDir, byte entityType )
    {
        File path = new File( storeDir, "lucene" );
        String extra = null;
        if ( entityType == LuceneCommand.NODE )
        {
            extra = "node";
        }
        else if ( entityType == LuceneCommand.RELATIONSHIP )
        {
            extra = "relationship";
        }
        else
        {
            throw new RuntimeException( "" + entityType );
        }
        return new File( path, extra );
    }
    
    static File getFileDirectory( String storeDir, IndexIdentifier identifier )
    {
        return new File( getFileDirectory( storeDir, identifier.entityTypeByte ),
                identifier.indexName );
    }
    
    static Directory getDirectory( String storeDir,
            IndexIdentifier identifier ) throws IOException
    {
        return FSDirectory.open( getFileDirectory( storeDir, identifier) );
    }
    
    static TopFieldCollector scoringCollector( Sort sorting, int n ) throws IOException
    {
        return TopFieldCollector.create( sorting, n, false, true, false, true );
    }
    
    synchronized IndexSearcherRef getIndexSearcher( IndexIdentifier identifier, boolean incRef )
    {
        try
        {
            IndexSearcherRef searcher = indexSearchers.get( identifier );
            if ( searcher == null )
            {
                IndexWriter writer = getIndexWriter( identifier );
                IndexReader reader = writer.getReader();
                IndexSearcher indexSearcher = new IndexSearcher( reader );
                searcher = new IndexSearcherRef( identifier, indexSearcher );
                indexSearchers.put( identifier, searcher );
            }
            else
            {
                Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy> writer = indexWriters.get( identifier );
                if ( writer != null && writer.second().compareAndSet( true, false ) )
                {
                    searcher = refreshSearcher( searcher );
                    if ( searcher != null )
                    {
                        indexSearchers.put( identifier, searcher );
                    }
                }
            }
            if ( incRef )
            {
                searcher.incRef();
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

    synchronized void invalidateIndexSearcher( IndexIdentifier identifier )
    {
        Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy> writer = indexWriters.get( identifier );
        if ( writer != null )
        {
            writer.second().set( true );
        }
    }

    void deleteIndex( IndexIdentifier identifier, boolean recovery )
    {
        closeWriter( identifier );
        deleteFileOrDirectory( getFileDirectory( baseStorePath, identifier ) );
        invalidateCache( identifier );
        boolean removeFromIndexStore = !recovery || (recovery &&
                indexStore.has( identifier.entityType.getType(), identifier.indexName ));
        if ( removeFromIndexStore )
        {
            indexStore.remove( identifier.entityType.getType(), identifier.indexName );
        }
        typeCache.invalidate( identifier );
        synchronized ( indexes )
        {
            LuceneIndex<? extends PropertyContainer> index = indexes.remove( identifier );
            if ( index != null )
            {
                index.markAsDeleted();
            }
        }
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
        Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy> writer = indexWriters.get( identifier );
        if ( writer != null )
        {
            return writer.first();
        }
        
        try
        {
            Directory dir = getDirectory( baseStorePath, identifier );
            directoryExists( dir );
            IndexType type = getType( identifier );
            SnapshotDeletionPolicy deletionPolicy = new MultipleBackupDeletionPolicy();
            IndexWriter indexWriter = new IndexWriter( dir, type.analyzer, deletionPolicy, MaxFieldLength.UNLIMITED );
            writer = Triplet.of( indexWriter, new AtomicBoolean(), deletionPolicy );
            Similarity similarity = type.getSimilarity();
            if ( similarity != null )
            {
                writer.first().setSimilarity( similarity );
            }
            
            // TODO We should tamper with this value and see how it affects the
            // general performance. Lucene docs says rather <10 for mixed
            // reads/writes 
//            writer.setMergeFactor( 8 );
            
            indexWriters.put( identifier, writer );
            return writer.first();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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
    
    private synchronized void closeWriter( IndexIdentifier identifier )
    {
        try
        {
            IndexSearcherRef searcher = indexSearchers.remove( identifier );
            Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy> writer = indexWriters.remove( identifier );
            if ( searcher != null )
            {
                searcher.dispose();
            }
            if ( writer != null )
            {
                writer.first().close();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to close lucene writer " + identifier, e );
        }
    }

    LruCache<String,Collection<Long>> getFromCache( IndexIdentifier identifier, String key )
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
        return providerStore.getCreationTime();
    }
    
    @Override
    public long getRandomIdentifier()
    {
        return providerStore.getRandomNumber();
    }
    
    @Override
    public long getCurrentLogVersion()
    {
        return providerStore.getVersion();
    }
    
    @Override
    public long getLastCommittedTxId()
    {
        return providerStore.getLastCommittedTx();
    }

    public void setLastCommittedTxId( long txId )
    {
        providerStore.setLastCommittedTx( txId );
    }
    
    @Override
    public XaContainer getXaContainer()
    {
        return this.xaContainer;
    }
    
    @Override
    public ClosableIterable<File> listStoreFiles() throws IOException
    {
        final Collection<File> files = new ArrayList<File>();
        final Collection<SnapshotDeletionPolicy> snapshots = new ArrayList<SnapshotDeletionPolicy>();
        for ( Map.Entry<IndexIdentifier, Triplet<IndexWriter, AtomicBoolean, SnapshotDeletionPolicy>> writer : indexWriters.entrySet() )
        {
            SnapshotDeletionPolicy deletionPolicy = writer.getValue().third();
            File indexDirectory = getFileDirectory( baseStorePath, writer.getKey() );
            for ( String fileName : deletionPolicy.snapshot().getFileNames() )
            {
                files.add( new File( indexDirectory, fileName ) );
            }
            snapshots.add( deletionPolicy );
        }
        files.add( providerStore.getFile() );
        return new ClosableIterable<File>()
        {
            public Iterator<File> iterator()
            {
                return files.iterator();
            }

            public void close()
            {
                for ( SnapshotDeletionPolicy deletionPolicy : snapshots )
                {
                    deletionPolicy.release();
                }
            }
        };
    }
}
