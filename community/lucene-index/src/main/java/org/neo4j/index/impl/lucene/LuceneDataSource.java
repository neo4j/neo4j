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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.index.impl.lucene.MultipleBackupDeletionPolicy.SNAPSHOT_ID;

/**
 * An DataSource optimized for the {@link LuceneIndexImplementation}.
 */
public class LuceneDataSource extends LifecycleAdapter
{
    public static abstract class Configuration
    {
        public static final Setting<Integer> lucene_searcher_cache_size = GraphDatabaseSettings.lucene_searcher_cache_size;
        public static final Setting<Boolean> ephemeral = GraphDatabaseFacadeFactory.Configuration.ephemeral;
    }

    public static final Version LUCENE_VERSION = Version.LUCENE_36;

    /**
     * Default {@link Analyzer} for fulltext parsing.
     */
    public static final Analyzer LOWER_CASE_WHITESPACE_ANALYZER = new Analyzer()
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
    private final File storeDir;
    private final Config config;
    private final FileSystemAbstraction fileSystemAbstraction;
    private IndexClockCache indexSearchers;
    private File baseStorePath;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    final IndexConfigStore indexStore;
    private IndexTypeCache typeCache;
    private boolean readOnly;
    private boolean closed;
    private LuceneFilesystemFacade filesystemFacade;
    private IndexReferenceFactory indexReferenceFactory;

    /**
     * Constructs this data source.
     */
    public LuceneDataSource( File storeDir, Config config, IndexConfigStore indexStore, FileSystemAbstraction fileSystemAbstraction )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.indexStore = indexStore;
        this.typeCache = new IndexTypeCache( indexStore );
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    @Override
    public void init()
    {
        this.filesystemFacade = config.get( Configuration.ephemeral ) ? LuceneFilesystemFacade.MEMORY
                : LuceneFilesystemFacade.FS;
        readOnly = config.get( GraphDatabaseSettings.read_only );
        indexSearchers = new IndexClockCache( config.get( Configuration.lucene_searcher_cache_size ) );
        this.baseStorePath = this.filesystemFacade.ensureDirectoryExists( fileSystemAbstraction,
                baseDirectory( storeDir ) );
        this.filesystemFacade.cleanWriteLocks( baseStorePath );
        this.typeCache = new IndexTypeCache( indexStore );
        this.indexReferenceFactory = readOnly ?
                                     new ReadOnlyIndexReferenceFactory( filesystemFacade, baseStorePath, typeCache ) :
                                     new WritableIndexReferenceFactory( filesystemFacade, baseStorePath, typeCache );
        closed = false;
    }

    private File baseDirectory( File storeDir )
    {
        return new File( storeDir, "index" );
    }

    IndexType getType( IndexIdentifier identifier, boolean recovery )
    {
        return typeCache.getIndexType( identifier, recovery );
    }

    @Override
    public void shutdown() throws IOException
    {
        synchronized ( this )
        {
            if ( closed )
            {
                return;
            }
            closed = true;
            for ( IndexReference searcher : indexSearchers.values() )
            {
                searcher.dispose();
            }
            indexSearchers.clear();
        }
    }

    private synchronized IndexReference[] getAllIndexes()
    {
        Collection<IndexReference> indexReferences = indexSearchers.values();
        return indexReferences.toArray( new IndexReference[indexReferences.size()] );
    }

    void force()
    {
        if ( readOnly )
        {
            return;
        }
        for ( IndexReference index : getAllIndexes() )
        {
            try
            {
                index.getWriter().commit();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to commit changes to " + index.getIdentifier() + " in " +
                        config.get( GraphDatabaseSettings.store_dir ).getAbsolutePath(), e );
            }
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

    static File getFileDirectory( File storeDir, IndexEntityType type )
    {
        File path = new File( storeDir, "lucene" );
        String extra = type.nameToLowerCase();
        return new File( path, extra );
    }

    static File getFileDirectory( File storeDir, IndexIdentifier identifier )
    {
        return new File( getFileDirectory( storeDir, identifier.entityType ), identifier.indexName );
    }

    static Directory getDirectory( File storeDir, IndexIdentifier identifier ) throws IOException
    {
        return FSDirectory.open( getFileDirectory( storeDir, identifier ) );
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
        {
            throw new IllegalStateException( "Lucene index provider has been shut down" );
        }
    }

    synchronized IndexReference syncGetIndexSearcher( IndexIdentifier identifier )
    {
        try
        {
            IndexReference indexReference = indexSearchers.get( identifier );
            if ( indexReference == null )
            {
                indexReference = indexReferenceFactory.createIndexReference( identifier );
                indexSearchers.put( identifier, indexReference );
            }
            else
            {
                if ( !readOnly )
                {
                    synchronized ( indexReference )
                    {
                        indexReference = refreshSearcherIfNeeded( indexReference );
                    }
                }
            }
            indexReference.incRef();
            return indexReference;
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
            searcher = indexReferenceFactory.refresh( searcher );
            if ( searcher != null )
            {
                indexSearchers.put( searcher.getIdentifier(), searcher );
            }
        }
        return searcher;
    }

    void invalidateIndexSearcher( IndexIdentifier identifier )
    {
        IndexReference searcher = indexSearchers.get( identifier );
        if ( searcher != null )
        {
            searcher.setStale();
        }
    }

    void deleteIndex( IndexIdentifier identifier, boolean recovery ) throws IOException
    {
        if ( readOnly )
        {
            throw new IllegalStateException( "Index deletion in read only mode is not supported." );
        }
        closeIndex( identifier );
        FileUtils.deleteRecursively( getFileDirectory( baseStorePath, identifier ) );
        boolean removeFromIndexStore =
                !recovery || (indexStore.has( identifier.entityType.entityClass(), identifier.indexName ));
        if ( removeFromIndexStore )
        {
            indexStore.remove( identifier.entityType.entityClass(), identifier.indexName );
        }
        typeCache.invalidate( identifier );
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
            if ( !(LuceneIndex.KEY_DOC_ID.equals( field.name() ) || LuceneIndex.KEY_END_NODE_ID.equals( field.name() ) || LuceneIndex.KEY_START_NODE_ID
                    .equals( field.name() )) )
            {
                return false;
            }
        }
        return true;
    }

    private synchronized void closeIndex( IndexIdentifier identifier )
    {
        try
        {
            IndexReference searcher = indexSearchers.remove( identifier );
            if ( searcher != null )
            {
                searcher.dispose();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to close lucene writer " + identifier, e );
        }
    }

    public ResourceIterator<File> listStoreFiles( boolean includeLogicalLogs ) throws IOException
    { // Never include logical logs since they are of little importance
        final Collection<File> files = new ArrayList<>();
        final Collection<SnapshotDeletionPolicy> snapshots = new ArrayList<>();
        makeSureAllIndexesAreInstantiated();
        for ( IndexReference writer : getAllIndexes() )
        {
            SnapshotDeletionPolicy deletionPolicy = (SnapshotDeletionPolicy) writer.getWriter().getConfig()
                    .getIndexDeletionPolicy();
            File indexDirectory = getFileDirectory( baseStorePath, writer.getIdentifier() );
            IndexCommit commit;
            try
            {
                // Throws IllegalStateException if no commits yet
                commit = deletionPolicy.snapshot( SNAPSHOT_ID );
            }
            catch ( IllegalStateException e )
            {
                /*
                 * This is insane but happens if we try to snapshot an existing index
                 * that has no commits. This is a bad API design - it should return null
                 * or something. This is not exceptional.
                 *
                 * For the time being we just do a commit and try again.
                 */
                writer.getWriter().commit();
                commit = deletionPolicy.snapshot( SNAPSHOT_ID );
            }

            for ( String fileName : commit.getFileNames() )
            {
                files.add( new File( indexDirectory, fileName ) );
            }
            snapshots.add( deletionPolicy );
        }
        return new PrefetchingResourceIterator<File>()
        {
            private final Iterator<File> filesIterator = files.iterator();

            @Override
            protected File fetchNextOrNull()
            {
                return filesIterator.hasNext() ? filesIterator.next() : null;
            }

            @Override
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

    public ResourceIterator<File> listStoreFiles() throws IOException
    {
        return listStoreFiles( false );
    }

    private void makeSureAllIndexesAreInstantiated()
    {
        for ( String name : indexStore.getNames( Node.class ) )
        {
            Map<String, String> config = indexStore.get( Node.class, name );
            if ( config.get( IndexManager.PROVIDER ).equals( LuceneIndexImplementation.SERVICE_NAME ) )
            {
                IndexIdentifier identifier = new IndexIdentifier( IndexEntityType.Node, name );
                getIndexSearcher( identifier );
            }
        }
        for ( String name : indexStore.getNames( Relationship.class ) )
        {
            Map<String, String> config = indexStore.get( Relationship.class, name );
            if ( config.get( IndexManager.PROVIDER ).equals( LuceneIndexImplementation.SERVICE_NAME ) )
            {
                IndexIdentifier identifier = new IndexIdentifier( IndexEntityType.Relationship, name );
                getIndexSearcher( identifier );
            }
        }
    }

    enum LuceneFilesystemFacade
    {
        FS
        {
            @Override
            Directory getDirectory( File baseStorePath, IndexIdentifier identifier ) throws IOException
            {
                return FSDirectory.open( getFileDirectory( baseStorePath, identifier ) );
            }

            @Override
            void cleanWriteLocks( File dir )
            {
                if ( !dir.isDirectory() )
                {
                    return;
                }
                for ( File file : dir.listFiles() )
                {
                    if ( file.isDirectory() )
                    {
                        cleanWriteLocks( file );
                    }
                    else if ( file.getName().equals( "write.lock" ) )
                    {
                        boolean success = file.delete();
                        assert success;
                    }
                }
            }

            @Override
            File ensureDirectoryExists( FileSystemAbstraction fileSystem, File dir )
            {
                if ( !dir.exists() && !dir.mkdirs() )
                {
                    String message = String.format( "Unable to create directory path[%s] for Neo4j store" + ".",
                            dir.getAbsolutePath() );
                    throw new RuntimeException( message );
                }
                return dir;
            }
        },
        MEMORY
        {
            @Override
            Directory getDirectory( File baseStorePath, IndexIdentifier identifier )
            {
                return new RAMDirectory();
            }

            @Override
            void cleanWriteLocks( File path )
            {
            }

            @Override
            File ensureDirectoryExists( FileSystemAbstraction fileSystem, File path )
            {
                try
                {
                    fileSystem.mkdirs( path );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                return path;
            }
        };
        abstract Directory getDirectory( File baseStorePath, IndexIdentifier identifier ) throws IOException;

        abstract File ensureDirectoryExists( FileSystemAbstraction fileSystem, File path );

        abstract void cleanWriteLocks( File path );
    }
}
