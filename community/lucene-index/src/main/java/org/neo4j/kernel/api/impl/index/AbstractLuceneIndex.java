/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.backup.WritableIndexSnapshotFileIterator;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

import static java.util.stream.Collectors.toList;

/**
 * Abstract implementation of a partitioned index.
 * Such index may consist of one or multiple separate Lucene indexes that are represented as independent
 * {@link AbstractIndexPartition partitions}.
 * Class and it's subclasses should not be directly used, instead please use corresponding writable or read only
 * wrapper.
 * @see WritableAbstractDatabaseIndex
 * @see ReadOnlyAbstractDatabaseIndex
 */
public abstract class AbstractLuceneIndex
{
    protected final PartitionedIndexStorage indexStorage;
    private final IndexPartitionFactory partitionFactory;

    // Note that we rely on the thread-safe internal snapshot feature of the CopyOnWriteArrayList
    // for the thread-safety of this and derived classes.
    private CopyOnWriteArrayList<AbstractIndexPartition> partitions = new CopyOnWriteArrayList<>();

    private volatile boolean open;

    public AbstractLuceneIndex( PartitionedIndexStorage indexStorage, IndexPartitionFactory partitionFactory )
    {
        this.indexStorage = indexStorage;
        this.partitionFactory = partitionFactory;
    }

    /**
     * Creates new index.
     * As part of creation process index will allocate all required folders, index failure storage
     * and will create its first partition.
     * <p>
     * <b>Index creation do not automatically open it. To be able to use index please open it first.</b>
     *
     * @throws IOException
     */
    public void create() throws IOException
    {
        ensureNotOpen();
        indexStorage.prepareFolder( indexStorage.getIndexFolder() );
        indexStorage.reserveIndexFailureStorage();
        createNewPartitionFolder();
    }

    /**
     * Open index with all allocated partitions.
     *
     * @throws IOException
     */
    public void open() throws IOException
    {
        Set<Map.Entry<File,Directory>> indexDirectories = indexStorage.openIndexDirectories().entrySet();
        List<AbstractIndexPartition> list = new ArrayList<>( indexDirectories.size() );
        for ( Map.Entry<File,Directory> entry : indexDirectories )
        {
            list.add( partitionFactory.createPartition( entry.getKey(), entry.getValue() ) );
        }
        partitions.addAll( list );
        open = true;
    }

    public boolean isOpen()
    {
        return open;
    }

    /**
     * Check lucene index existence within all allocated partitions.
     *
     * @return true if index exist in all partitions, false when index is empty or does not exist
     * @throws IOException
     */
    public boolean exists() throws IOException
    {
        List<File> folders = indexStorage.listFolders();
        if ( folders.isEmpty() )
        {
            return false;
        }
        for ( File folder : folders )
        {
            if ( !luceneDirectoryExists( folder ) )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Verify state of the index.
     * If index is already open and in use method assume that index is valid since lucene already operating with it,
     * otherwise necessary checks perform.
     *
     * @return true if lucene confirm that index is in valid clean state or index is already open.
     */
    public boolean isValid()
    {
        if ( open )
        {
            return true;
        }
        Collection<Directory> directories = null;
        try
        {
            directories = indexStorage.openIndexDirectories().values();
            for ( Directory directory : directories )
            {
                // it is ok for index directory to be empty
                // this can happen if it is opened and closed without any writes in between
                if ( !ArrayUtil.isEmpty( directory.listAll() ) )
                {
                    try ( CheckIndex checker = new CheckIndex( directory ) )
                    {
                        CheckIndex.Status status = checker.checkIndex();
                        if ( !status.clean )
                        {
                            return false;
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            return false;
        }
        finally
        {
            IOUtils.closeAllSilently( directories );
        }
        return true;
    }

    /**
     * Close index and deletes all it's partitions.
     *
     * @throws IOException
     */
    public void drop() throws IOException
    {
        close();
        indexStorage.cleanupFolder( indexStorage.getIndexFolder() );
    }

    /**
     * Commits all index partitions.
     *
     * @param merge also merge all segments together. This should be done before reading term frequencies.
     * @throws IOException on Lucene I/O error.
     */
    public void flush( boolean merge ) throws IOException
    {
        List<AbstractIndexPartition> partitions = getPartitions();
        for ( AbstractIndexPartition partition : partitions )
        {
            IndexWriter writer = partition.getIndexWriter();
            writer.commit();
            if ( merge )
            {
                writer.forceMerge( 1 );
            }
        }
    }

    public void close() throws IOException
    {
        open = false;
        IOUtils.closeAll( partitions );
        partitions.clear();
    }

    /**
     * Creates an iterable over all {@link org.apache.lucene.document.Document document}s in all partitions.
     *
     * @return LuceneAllDocumentsReader over all documents
     */
    public LuceneAllDocumentsReader allDocumentsReader()
    {
        ensureOpen();
        List<PartitionSearcher> searchers = new ArrayList<>( partitions.size() );
        try
        {
            for ( AbstractIndexPartition partition : partitions )
            {
                searchers.add( partition.acquireSearcher() );
            }

            List<LucenePartitionAllDocumentsReader> partitionReaders = searchers.stream()
                    .map( LucenePartitionAllDocumentsReader::new )
                    .collect( toList() );

            return new LuceneAllDocumentsReader( partitionReaders );
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( searchers );
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Snapshot of all file in all index partitions.
     *
     * @return iterator over all index files.
     * @throws IOException
     * @see WritableIndexSnapshotFileIterator
     */
    public ResourceIterator<File> snapshot() throws IOException
    {
        ensureOpen();
        List<ResourceIterator<File>> snapshotIterators = null;
        try
        {
            List<AbstractIndexPartition> partitions = getPartitions();
            snapshotIterators = new ArrayList<>( partitions.size() );
            for ( AbstractIndexPartition partition : partitions )
            {
                snapshotIterators.add( partition.snapshot() );
            }
            return Iterators.concatResourceIterators( snapshotIterators.iterator() );
        }
        catch ( Exception e )
        {
            if ( snapshotIterators != null )
            {
                try
                {
                    IOUtils.closeAll( snapshotIterators );
                }
                catch ( IOException ex )
                {
                    e.addSuppressed( ex );
                }
            }
            throw e;
        }
    }

    /**
     * Refresh all partitions to make newly inserted data visible for readers.
     *
     * @throws IOException
     */
    public void maybeRefreshBlocking() throws IOException
    {
        try
        {
            getPartitions().parallelStream().forEach( this::maybeRefreshPartition );
        }
        catch ( UncheckedIOException e )
        {
            throw e.getCause();
        }
    }

    private void maybeRefreshPartition( AbstractIndexPartition partition )
    {
        try
        {
            partition.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public List<AbstractIndexPartition> getPartitions()
    {
        ensureOpen();
        return partitions;
    }

    public boolean hasSinglePartition( List<AbstractIndexPartition> partitions )
    {
        return partitions.size() == 1;
    }

    public AbstractIndexPartition getFirstPartition( List<AbstractIndexPartition> partitions )
    {
        return partitions.get( 0 );
    }

    /**
     * Add new partition to the index.
     *
     * @return newly created partition
     * @throws IOException
     */
    AbstractIndexPartition addNewPartition() throws IOException
    {
        ensureOpen();
        File partitionFolder = createNewPartitionFolder();
        Directory directory = indexStorage.openDirectory( partitionFolder );
        AbstractIndexPartition indexPartition = partitionFactory.createPartition( partitionFolder, directory );
        partitions.add( indexPartition );
        return indexPartition;
    }

    protected void ensureOpen()
    {
        if ( !open )
        {
            throw new IllegalStateException( "Please open lucene index before working with it." );
        }
    }

    protected void ensureNotOpen()
    {
        if ( open )
        {
            throw new IllegalStateException( "Lucene index should not be open to be able to perform required " +
                                             "operation." );
        }
    }

    protected static List<PartitionSearcher> acquireSearchers( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = new ArrayList<>( partitions.size() );
        try
        {
            for ( AbstractIndexPartition partition : partitions )
            {
                searchers.add( partition.acquireSearcher() );
            }
            return searchers;
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( searchers );
            throw e;
        }
    }

    private boolean luceneDirectoryExists( File folder ) throws IOException
    {
        try ( Directory directory = indexStorage.openDirectory( folder ) )
        {
            return DirectoryReader.indexExists( directory );
        }
    }

    private File createNewPartitionFolder() throws IOException
    {
        File partitionFolder = indexStorage.getPartitionFolder( partitions.size() + 1 );
        indexStorage.prepareFolder( partitionFolder );
        return partitionFolder;
    }
}
