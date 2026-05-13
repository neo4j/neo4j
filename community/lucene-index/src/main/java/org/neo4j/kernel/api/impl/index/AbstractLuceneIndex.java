/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index;

import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.IndexFileSnapshotter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneAllDocumentsReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.writer.LucenePartitionIndexWriter;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.logging.LogProvider;

/**
 * Abstract implementation of a partitioned index.
 * Such index may consist of one or multiple separate Lucene indexes that are represented as independent
 * {@link AbstractIndexPartition partitions}.
 * Class and it's subclasses should not be directly used, instead please use corresponding writable or droppable
 * wrapper.
 * @see WritableDatabaseIndex
 * @see MinimalDatabaseIndex
 */
public abstract class AbstractLuceneIndex<READER extends IndexReader> implements IndexFileSnapshotter {
    protected final PartitionedIndexStorage indexStorage;
    protected final IndexDescriptor descriptor;
    private final IndexPartitionFactory partitionFactory;
    private final Config config;
    protected final LogProvider logProvider;

    // Note that we rely on the thread-safe internal snapshot feature of the CopyOnWriteArrayList
    // for the thread-safety of this and derived classes.
    private final List<AbstractIndexPartition> partitions = new CopyOnWriteArrayList<>();

    private volatile boolean open;

    public AbstractLuceneIndex(
            PartitionedIndexStorage indexStorage,
            IndexPartitionFactory partitionFactory,
            IndexDescriptor descriptor,
            Config config,
            LogProvider logProvider) {
        this.indexStorage = indexStorage;
        this.partitionFactory = partitionFactory;
        this.descriptor = descriptor;
        this.config = config;
        this.logProvider = logProvider;
    }

    public LuceneContext luceneContext() {
        return indexStorage.luceneContext();
    }

    /**
     * Creates new index.
     * As part of creation process index will allocate all required folders, index failure storage
     * and will create its first partition.
     * <p>
     * <b>Index creation do not automatically open it. To be able to use index please open it first.</b>
     */
    public void create() throws IOException {
        ensureNotOpen();
        indexStorage.prepareFolder(indexStorage.getIndexFolder());
        indexStorage.reserveIndexFailureStorage();
        createNewPartitionFolder();
    }

    /**
     * Open index with all allocated partitions.
     */
    public void open() throws IOException {
        Set<Map.Entry<Path, LuceneDirectory>> indexDirectories =
                indexStorage.openIndexDirectories().entrySet();
        Collection<AbstractIndexPartition> list = new ArrayList<>(indexDirectories.size());
        for (Map.Entry<Path, LuceneDirectory> entry : indexDirectories) {
            list.add(partitionFactory.createPartition(entry.getKey(), entry.getValue()));
        }
        partitions.addAll(list);
        open = true;
    }

    public boolean isOpen() {
        return open;
    }

    /**
     * Check lucene index existence within all allocated partitions.
     *
     * @return true if index exist in all partitions, false when index is empty or does not exist
     */
    public boolean exists() throws IOException {
        List<Path> folders = indexStorage.listFolders();
        if (folders.isEmpty()) {
            return false;
        }
        for (Path folder : folders) {
            if (!luceneDirectoryExists(folder)) {
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
    public boolean isValid() {
        if (open) {
            return true;
        }
        Collection<LuceneDirectory> directories = null;
        try {
            directories = indexStorage.openIndexDirectories().values();
            for (LuceneDirectory directory : directories) {
                if (!directory.checkIndexIsClean()) {
                    return false;
                }
            }
        } catch (IOException e) {
            return false;
        } finally {
            if (directories != null) {
                IOUtils.closeAllSilently(directories);
            }
        }
        return true;
    }

    public LucenePartitionIndexWriter getIndexWriter(WritableDatabaseIndex<?, ?> writableDatabaseIndex) {
        ensureOpen();
        return new PartitionedIndexWriter(writableDatabaseIndex, config);
    }

    public READER getIndexReader(IndexUsageTracking usageTracker) throws IOException {
        ensureOpen();
        List<AbstractIndexPartition> partitions = getPartitions();
        return hasSinglePartition(partitions)
                ? createSimpleReader(partitions, usageTracker)
                : createPartitionedReader(partitions, usageTracker);
    }

    public IndexDescriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Close index and deletes all it's partitions.
     */
    public void drop() {
        try {
            close();
            indexStorage.cleanupFolder(indexStorage.getIndexFolder());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Commits all index partitions.
     *
     * @param merge also merge all segments together. This should be done before reading term frequencies.
     * @throws IOException on Lucene I/O error.
     */
    public void flush(boolean merge) throws IOException {
        List<AbstractIndexPartition> partitions = getPartitions();
        for (AbstractIndexPartition partition : partitions) {
            LuceneIndexWriter writer = partition.getIndexWriter();
            writer.commit();
            if (merge) {
                writer.forceMerge(1);
            }
        }
    }

    public void close() throws IOException {
        open = false;
        IOUtils.closeAll(partitions);
        partitions.clear();
    }

    /**
     * Creates an iterable over all {@link LuceneDocument document}s in all partitions.
     *
     * @return LuceneAllDocumentsReader over all documents
     */
    public LucenePartitionsAllDocumentsReader allDocumentsReader() {
        ensureOpen();
        List<SearcherReference> searchers = new ArrayList<>(partitions.size());
        try {
            for (AbstractIndexPartition partition : partitions) {
                searchers.add(partition.acquireSearcher());
            }

            List<LuceneAllDocumentsReader> partitionReaders = searchers.stream()
                    .map(SearcherReference::getIndexSearcher)
                    .map(LuceneIndexSearcher::newAllDocumentsReader)
                    .toList();

            return new LucenePartitionsAllDocumentsReader(partitionReaders, searchers);
        } catch (IOException e) {
            IOUtils.closeAllSilently(searchers);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Snapshot of all file in all index partitions.
     *
     * @return iterator over all index files.
     */
    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        if (indexStorage.getStoredIndexFailure() != null) {
            return asResourceIterator(iterator(indexStorage.getIndexFailureFile()));
        }

        ensureOpen();
        List<ResourceIterator<Path>> snapshotIterators = null;
        try {
            List<AbstractIndexPartition> partitions = getPartitions();
            snapshotIterators = new ArrayList<>(partitions.size());
            for (AbstractIndexPartition partition : partitions) {
                snapshotIterators.add(partition.snapshot());
            }
            return Iterators.concatResourceIterators(snapshotIterators.iterator());
        } catch (Exception e) {
            if (snapshotIterators != null) {
                try {
                    IOUtils.closeAll(snapshotIterators);
                } catch (IOException ex) {
                    e.addSuppressed(ex);
                }
            }
            throw e;
        }
    }

    /**
     * Refresh all partitions to make newly inserted data visible for readers.
     */
    public void maybeRefreshBlocking() throws IOException {
        try {
            getPartitions().parallelStream().forEach(AbstractLuceneIndex::maybeRefreshPartition);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static void maybeRefreshPartition(AbstractIndexPartition partition) {
        try {
            partition.maybeRefreshBlocking();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<AbstractIndexPartition> getPartitions() {
        ensureOpen();
        return partitions;
    }

    public static boolean hasSinglePartition(Collection<AbstractIndexPartition> partitions) {
        return partitions.size() == 1;
    }

    public static AbstractIndexPartition getFirstPartition(List<AbstractIndexPartition> partitions) {
        return partitions.getFirst();
    }

    /**
     * Add new partition to the index.
     *
     * @return newly created partition
     */
    AbstractIndexPartition addNewPartition() throws IOException {
        ensureOpen();
        Path partitionFolder = createNewPartitionFolder();
        LuceneDirectory directory = indexStorage.openDirectory(partitionFolder);
        AbstractIndexPartition indexPartition = partitionFactory.createPartition(partitionFolder, directory);
        partitions.add(indexPartition);
        return indexPartition;
    }

    protected void ensureOpen() {
        if (!open) {
            throw new IllegalStateException("Please open lucene index before working with it.");
        }
    }

    protected void ensureNotOpen() {
        if (open) {
            throw new IllegalStateException(
                    "Lucene index should not be open to be able to perform required " + "operation.");
        }
    }

    protected static List<SearcherReference> acquireSearchers(Collection<AbstractIndexPartition> partitions)
            throws IOException {
        List<SearcherReference> searchers = new ArrayList<>(partitions.size());
        try {
            for (AbstractIndexPartition partition : partitions) {
                searchers.add(partition.acquireSearcher());
            }
            return searchers;
        } catch (IOException e) {
            IOUtils.closeAllSilently(searchers);
            throw e;
        }
    }

    private boolean luceneDirectoryExists(Path folder) throws IOException {
        try (LuceneDirectory directory = indexStorage.openDirectory(folder)) {
            return directory.indexExists();
        }
    }

    private Path createNewPartitionFolder() throws IOException {
        Path partitionFolder = indexStorage.getPartitionFolder(partitions.size() + 1);
        indexStorage.prepareFolder(partitionFolder);
        return partitionFolder;
    }

    /**
     * Check if this index is marked as online.
     *
     * @return <code>true</code> if index is online, <code>false</code> otherwise
     */
    public boolean isOnline() throws IOException {
        ensureOpen();
        AbstractIndexPartition partition = getFirstPartition(getPartitions());
        LuceneDirectory directory = partition.getDirectory();
        try (LuceneDirectoryReader reader = directory.open()) {
            return reader.isOnline();
        }
    }

    /**
     * Marks index as online by including "status" -> "online" map into commit metadata of the first partition.
     */
    public void markAsOnline() throws IOException {
        ensureOpen();
        if (getPartitions().isEmpty()) {
            addNewPartition();
        }
        AbstractIndexPartition partition = getFirstPartition(getPartitions());
        LuceneIndexWriter indexWriter = partition.getIndexWriter();
        indexWriter.markAsOnline();
        flush(false);
    }

    /**
     * Writes the given failure message to the failure storage.
     *
     * @param failure the failure message.
     */
    public void markAsFailed(String failure) throws IOException {
        indexStorage.storeIndexFailure(failure);
    }

    protected abstract READER createSimpleReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException;

    protected abstract READER createPartitionedReader(
            List<AbstractIndexPartition> partitions, IndexUsageTracking usageTracker) throws IOException;

    /**
     * Allows the visitor to access the underlying directories that makes up this index.
     * Any writer is closed during this access.
     *
     * @param visitor that gets access to the raw directories of the index.
     * @throws IOException on I/O error.
     */
    protected void accessClosedDirectories(ThrowingBiConsumer<Integer, LuceneDirectory, IOException> visitor)
            throws IOException {
        for (AbstractIndexPartition partition : getPartitions()) {
            partition.accessClosedDirectory(visitor);
        }
    }

    long sizeInBytes() {
        return indexStorage.sizeInBytes();
    }
}
