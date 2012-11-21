/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.index.impl.lucene.MultipleBackupDeletionPolicy.SNAPSHOT_ID;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionStringToLong;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.ClosableIterable;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.index.IndexProviderStore;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;

/**
 * An {@link XaDataSource} optimized for the {@link LuceneIndexImplementation}.
 * This class is public because the XA framework requires it.
 */
public class LuceneDataSource extends LogBackedXaDataSource
{
    private final Config config;
    private FileSystemAbstraction fileSystemAbstraction;

    public static abstract class Configuration
        extends LogBackedXaDataSource.Configuration
    {
        public static final GraphDatabaseSetting.IntegerSetting lucene_searcher_cache_size = GraphDatabaseSettings.lucene_searcher_cache_size;

        public static final GraphDatabaseSetting.BooleanSetting read_only = GraphDatabaseSettings.read_only;
        public static final GraphDatabaseSetting.BooleanSetting allow_store_upgrade = GraphDatabaseSettings.allow_store_upgrade;

        public static final GraphDatabaseSetting.BooleanSetting ephemeral = InternalAbstractGraphDatabase.Configuration.ephemeral;
        public static final GraphDatabaseSetting.StringSetting store_dir = NeoStoreXaDataSource.Configuration.store_dir;
    }

    public static final Version LUCENE_VERSION = Version.LUCENE_35;
    public static final String DEFAULT_NAME = "lucene-index";
    public static final byte[] DEFAULT_BRANCH_ID = UTF8.encode( "162374" );
    public static final long INDEX_VERSION = versionStringToLong( "3.5" );

    /**
     * Default {@link Analyzer} for fulltext parsing.
     */
    public static final Analyzer LOWER_CASE_WHITESPACE_ANALYZER =
        new Analyzer()
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new LowerCaseFilter( LUCENE_VERSION, new WhitespaceTokenizer( LUCENE_VERSION, reader ) );
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
            return new WhitespaceTokenizer( LUCENE_VERSION, reader );
        }

        @Override
        public String toString()
        {
            return "WHITESPACE_ANALYZER";
        }
    };

    public static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    private IndexClockCache indexSearchers;
    private XaContainer xaContainer;
    private String baseStorePath;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    final IndexStore indexStore;
    private final XaFactory xaFactory;
    IndexProviderStore providerStore;
    private IndexTypeCache typeCache;
    private boolean closed;
    private Cache caching;
    EntityType nodeEntityType;
    EntityType relationshipEntityType;
    final Map<IndexIdentifier, LuceneIndex<? extends PropertyContainer>> indexes =
            new HashMap<IndexIdentifier, LuceneIndex<? extends PropertyContainer>>();
    private LuceneFilesystemFacade filesystemFacade;

    // Used for assertion after recovery has been completed.
    private final Set<IndexIdentifier> expectedFutureRecoveryDeletions = new HashSet<IndexIdentifier>();

    /**
     * Constructs this data source.
     *
     * @throws InstantiationException if the data source couldn't be
     * instantiated
     */
    public LuceneDataSource( Config config,  IndexStore indexStore, FileSystemAbstraction fileSystemAbstraction,
                             XaFactory xaFactory)
    {
        super( DEFAULT_BRANCH_ID, DEFAULT_NAME );
        this.config = config;
        this.indexStore = indexStore;
        this.xaFactory = xaFactory;
        this.typeCache = new IndexTypeCache( indexStore );
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    @Override
    public void init()
    {}

    @Override
    public void start()
    {
        this.filesystemFacade = config.get( Configuration.ephemeral ) ? LuceneFilesystemFacade.MEMORY : LuceneFilesystemFacade.FS;
        indexSearchers = new IndexClockCache( config.get( Configuration.lucene_searcher_cache_size ) );
        caching = new Cache();
        String storeDir = config.get( Configuration.store_dir );
        this.baseStorePath = this.filesystemFacade.ensureDirectoryExists( storeDir + File.separator + "index" );
        this.filesystemFacade.cleanWriteLocks( baseStorePath );
        boolean allowUpgrade = config.get( Configuration.allow_store_upgrade );
        this.providerStore = newIndexStore( baseStorePath, fileSystemAbstraction, allowUpgrade );
        this.typeCache = new IndexTypeCache( indexStore );
        boolean isReadOnly = config.get( Configuration.read_only );

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
        DependencyResolver dummy = new DependencyResolver()
        {
            @Override
            public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
            {
                return (T) LuceneDataSource.this.config;
            }
        };
        xaContainer = xaFactory.newXaContainer( this, this.baseStorePath + File.separator + "lucene.log", cf, tf,
                TransactionStateFactory.NO_STATE_FACTORY, new TransactionInterceptorProviders( new HashSet<TransactionInterceptorProvider>(), dummy ) );
        closed = false;
        if ( !isReadOnly )
        {
            try
            {
                xaContainer.openLogicalLog();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to open lucene log in " + this.baseStorePath, e );
            }

            setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );
        }
    }

    IndexType getType( IndexIdentifier identifier, boolean recovery )
    {
        return typeCache.getIndexType( identifier, recovery );
    }

    private IndexProviderStore newIndexStore( String dbStoreDir, FileSystemAbstraction fileSystem, boolean allowUpgrade )
    {
        String dir = dbStoreDir + File.separator + "lucene-store.db";
        File file = new File( dir  );
        return new IndexProviderStore( file, fileSystem, INDEX_VERSION, allowUpgrade );
    }

    @Override
    public void stop()
    {
        synchronized ( this )
        {
            super.stop();
            if ( closed )
            {
                return;
            }
            closed = true;
            for ( IndexReference searcher : indexSearchers.values() )
            {
                try
                {
                    searcher.dispose( true );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
            indexSearchers.clear();
        }

        if ( xaContainer != null )
        {
            xaContainer.close();
        }
        providerStore.close();
    }

    @Override
    public void shutdown()
    {}

    public Index<Node> nodeIndex( String indexName, GraphDatabaseService graphDb, LuceneIndexImplementation luceneIndexImplementation )
    {
        IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.NODE,
                nodeEntityType, indexName );
        synchronized ( indexes )
        {
            LuceneIndex index = indexes.get( identifier );
            if ( index == null )
            {
                index = new LuceneIndex.NodeIndex( luceneIndexImplementation, graphDb, identifier );
                indexes.put( identifier, index );
            }
            return index;
        }
    }

    public RelationshipIndex relationshipIndex( String indexName,
                                                GraphDatabaseService gdb,
                                                LuceneIndexImplementation luceneIndexImplementation
    )
    {
        IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.RELATIONSHIP,
                relationshipEntityType, indexName );
        synchronized ( indexes )
        {
            LuceneIndex index = indexes.get( identifier );
            if ( index == null )
            {
                index = new LuceneIndex.RelationshipIndex( luceneIndexImplementation, gdb, identifier );
                indexes.put( identifier, index );
            }
            return (RelationshipIndex) index;
        }
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
        public XaTransaction create( int identifier, TransactionState state )
        {
            return createTransaction( identifier, this.getLogicalLog() );
        }

        @Override
        public void flushAll()
        {
            for ( IndexReference index : getAllIndexes() )
            {
                try
                {
                    index.getWriter().commit();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "unable to commit changes to " + index.getIdentifier(), e );
                }
            }
            providerStore.flush();
        }
        
        @Override
        public void recoveryComplete()
        {
            if ( !expectedFutureRecoveryDeletions.isEmpty() )
                throw new TransactionFailureException( "Recovery discovered transactions which couldn't " +
                		"be applied due to a future index deletion, however some expected deletions " +
                		"weren't encountered: " + expectedFutureRecoveryDeletions );
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
        public void setVersion( long version )
        {
            providerStore.setVersion( version );
        }

        @Override
        public long getLastCommittedTx()
        {
            return providerStore.getLastCommittedTx();
        }
    }

    private synchronized IndexReference[] getAllIndexes()
    {
        Collection<IndexReference> indexReferences = indexSearchers.values();

        return indexReferences.toArray( new IndexReference[indexReferences.size()] );
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
     * or refreshed) {@code searcher} is returned. But if something has changed a
     * refreshed searcher is returned. It makes use if the
     * {@link IndexReader#openIfChanged(IndexReader, IndexWriter, boolean)} which faster than opening an index from
     * scratch.
     *
     * @param searcher the {@link IndexSearcher} to refresh.
     * @return a refreshed version of the searcher or, if nothing has changed,
     * {@code null}.
     * @throws IOException if there's a problem with the index.
     */
    private IndexReference refreshSearcher( IndexReference searcher )
    {
        try
        {
            IndexReader reader = searcher.getSearcher().getIndexReader();
            IndexWriter writer = searcher.getWriter();
            IndexReader reopened = IndexReader.openIfChanged( reader, writer, true );
            if ( reopened != null )
            {
                IndexSearcher newSearcher = new IndexSearcher( reopened );
                searcher.detachOrClose();
                return new IndexReference( searcher.getIdentifier(), newSearcher, writer );
            }
            return searcher;
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

    IndexReference getIndexSearcher( IndexIdentifier identifier )
    {
        assertNotClosed();
        IndexReference searcher = indexSearchers.get( identifier );
        if ( searcher == null )
        {
            return syncGetIndexSearcher( identifier );
        }

        synchronized ( searcher )
        {
            /*
             * We need to get again a reference to the searcher because it might be so that
             * it was refreshed while we waited. Once in here though no one will mess with
             * our searcher
             */
            searcher = indexSearchers.get( identifier );
            if ( searcher == null || searcher.isClosed() )
            {
                return syncGetIndexSearcher( identifier );
            }
            searcher = refreshSearcherIfNeeded( searcher );
            searcher.incRef();
            return searcher;
        }
    }

    private void assertNotClosed()
    {
        if ( closed )
            throw new IllegalStateException( "Lucene index provider has been shut down" );
    }

    synchronized IndexReference syncGetIndexSearcher( IndexIdentifier identifier )
    {
        try
        {
            IndexReference searcher = indexSearchers.get( identifier );
            if ( searcher == null )
            {
                IndexWriter writer = newIndexWriter( identifier );
                IndexReader reader = IndexReader.open( writer, true );
                IndexSearcher indexSearcher = new IndexSearcher( reader );
                searcher = new IndexReference( identifier, indexSearcher, writer );
                indexSearchers.put( identifier, searcher );
            }
            else
            {
                synchronized ( searcher )
                {
                    searcher = refreshSearcherIfNeeded( searcher );
                }
            }
            searcher.incRef();
            return searcher;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private IndexReference refreshSearcherIfNeeded( IndexReference searcher )
    {
        if ( searcher.checkAndClearStale() )
        {
            searcher = refreshSearcher( searcher );
            if ( searcher != null )
            {
                indexSearchers.put( searcher.getIdentifier(), searcher );
            }
        }
        return searcher;
    }

    XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog )
    {
        return new LuceneTransaction( identifier, logicalLog, this );
    }

    void invalidateIndexSearcher( IndexIdentifier identifier )
    {
        IndexReference searcher = indexSearchers.get( identifier );
        if ( searcher != null )
            searcher.setStale();
    }

    void deleteIndex( IndexIdentifier identifier, boolean recovery )
    {
        closeIndex( identifier );
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

    private /*synchronized elsewhere*/ IndexWriter newIndexWriter( IndexIdentifier identifier )
    {
        assertNotClosed();
        try
        {
            Directory dir = filesystemFacade.getDirectory( baseStorePath, identifier ); //getDirectory( baseStorePath, identifier );
            directoryExists( dir );
            IndexType type = getType( identifier, false );
            IndexWriterConfig writerConfig = new IndexWriterConfig( LUCENE_VERSION, type.analyzer );
            writerConfig.setIndexDeletionPolicy( new MultipleBackupDeletionPolicy() );
            Similarity similarity = type.getSimilarity();
            if ( similarity != null )
            {
                writerConfig.setSimilarity( similarity );
            }
            IndexWriter indexWriter = new IndexWriter( dir, writerConfig );

            // TODO We should tamper with this value and see how it affects the
            // general performance. Lucene docs says rather <10 for mixed
            // reads/writes
//            writer.setMergeFactor( 8 );

            return indexWriter;
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

    private synchronized void closeIndex( IndexIdentifier identifier )
    {
        try
        {
            IndexReference searcher = indexSearchers.remove( identifier );
            if ( searcher != null )
            {
                searcher.dispose( true );
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
        LruCache<String,Collection<Long>> cache = this.caching.get( identifier, key );
        return cache != null ? cache.maxSize() : null;
    }

    void invalidateCache( IndexIdentifier identifier, String key, Object value )
    {
        LruCache<String, Collection<Long>> cache = caching.get( identifier, key );
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

    @Override
    public void setLastCommittedTxId( long txId )
    {
        providerStore.setLastCommittedTx( txId );
    }

    @Override
    public XaContainer getXaContainer()
    {
        return this.xaContainer;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public ClosableIterable<File> listStoreFiles( boolean includeLogicalLogs ) throws IOException
    {   // Never include logical logs since they are of little importance
        final Collection<File> files = new ArrayList<File>();
        final Collection<SnapshotDeletionPolicy> snapshots = new ArrayList<SnapshotDeletionPolicy>();
        makeSureAllIndexesAreInstantiated();
        for ( IndexReference writer : getAllIndexes() )
        {
            SnapshotDeletionPolicy deletionPolicy = (SnapshotDeletionPolicy)
                    writer.getWriter().getConfig().getIndexDeletionPolicy();
            File indexDirectory = getFileDirectory( baseStorePath, writer.getIdentifier() );
            try
            {
                // Throws IllegalStateException if no commits yet
                IndexCommit commit = deletionPolicy.snapshot( SNAPSHOT_ID );
                for ( String fileName : commit.getFileNames() )
                {
                    files.add( new File( indexDirectory, fileName ) );
                }
                snapshots.add( deletionPolicy );
            }
            catch ( IllegalStateException e )
            {
                // TODO Review this
                /*
                 * This is insane but happens if we try to snapshot an existing index
                 * that has no commits. This is a bad API design - it should return null
                 * or something. This is not exceptional.
                 */
            }
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
                    try
                    {
                        deletionPolicy.release( SNAPSHOT_ID );
                    }
                    catch ( IOException e )
                    {
                        // TODO What to do?
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    private void makeSureAllIndexesAreInstantiated()
    {
        for ( String name : indexStore.getNames( Node.class ) )
        {
            Map<String, String> config = indexStore.get( Node.class, name );
            if ( config.get( IndexManager.PROVIDER ).equals( LuceneIndexImplementation.SERVICE_NAME ) )
            {
                IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.NODE, nodeEntityType, name );
                getIndexSearcher( identifier );
            }
        }
        for ( String name : indexStore.getNames( Relationship.class ) )
        {
            Map<String, String> config = indexStore.get( Relationship.class, name );
            if ( config.get( IndexManager.PROVIDER ).equals( LuceneIndexImplementation.SERVICE_NAME ) )
            {
                IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.RELATIONSHIP, relationshipEntityType, name );
                getIndexSearcher( identifier );
            }
        }
    }

    private static enum LuceneFilesystemFacade
    {
        FS
        {
            @Override
            Directory getDirectory( String baseStorePath, IndexIdentifier identifier ) throws IOException
            {
                return FSDirectory.open( getFileDirectory( baseStorePath, identifier) );
            }

            @Override
            void cleanWriteLocks( String directory )
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
                    } else if ( file.getName().equals( "write.lock" ) )
                    {
                        boolean success = file.delete();
                        assert success;
                    }
                }
            }

            @Override
            String ensureDirectoryExists( String path )
            {
                    File dir = new File( path);
                    if ( !dir.exists() )
                    {
                        if ( !dir.mkdirs() )
                        {
                            String message = String.format( "Unable to create directory path[%s] for Neo4j store.", dir.getAbsolutePath() );
                            throw new RuntimeException( message );
                        }
                    }
                    return dir.getAbsolutePath();

            }
        },
        MEMORY
        {
            @Override
            Directory getDirectory( String baseStorePath, IndexIdentifier identifier )
            {
                return new RAMDirectory();
            }

            @Override
            void cleanWriteLocks( String path )
            {
            }

            @Override
            String ensureDirectoryExists( String path )
            {
                return path;
            }
        };

        abstract Directory getDirectory( String baseStorePath, IndexIdentifier identifier ) throws IOException;
        abstract String ensureDirectoryExists( String path );
        abstract void cleanWriteLocks( String path);
    }

    void addExpectedFutureDeletion( IndexIdentifier identifier )
    {
        expectedFutureRecoveryDeletions.add( identifier );
    }
    
    void removeExpectedFutureDeletion( IndexIdentifier identifier )
    {
        expectedFutureRecoveryDeletions.remove( identifier );
    }
}
