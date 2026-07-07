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
package org.neo4j.kernel.api.impl.index.lucene.v9;

import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.impl.index.backup.ReadOnlyIndexSnapshotFileIterator;
import org.neo4j.kernel.api.impl.index.backup.SnapshotReleaseException;
import org.neo4j.kernel.api.impl.index.backup.UnsupportedIndexDeletionPolicy;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.shaded.lucene9.document.Document;
import org.neo4j.shaded.lucene9.index.DirectoryReader;
import org.neo4j.shaded.lucene9.index.IndexCommit;
import org.neo4j.shaded.lucene9.index.IndexDeletionPolicy;
import org.neo4j.shaded.lucene9.index.IndexWriter;
import org.neo4j.shaded.lucene9.index.LogMergePolicy;
import org.neo4j.shaded.lucene9.index.MergePolicy;
import org.neo4j.shaded.lucene9.index.SegmentInfos;
import org.neo4j.shaded.lucene9.index.SnapshotDeletionPolicy;
import org.neo4j.shaded.lucene9.index.Term;
import org.neo4j.shaded.lucene9.index.TieredMergePolicy;
import org.neo4j.shaded.lucene9.store.Directory;

class Lucene9IndexWriter implements LuceneIndexWriter {
    private final IndexWriter indexWriter;

    Lucene9IndexWriter(IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }

    @Override
    public LuceneDocument newDocument() {
        return new Lucene9Document();
    }

    @Override
    public void updateDocument(String idField, long id, LuceneDocument document) throws IOException {
        indexWriter.updateDocument(new Term(idField, Long.toString(id)), toInternalDocument(document));
    }

    @Override
    public void rollback() throws IOException {
        indexWriter.rollback();
    }

    @Override
    public LuceneDirectoryReader directoryReader() throws IOException {
        DirectoryReader directoryReader = DirectoryReader.open(indexWriter, true, false);
        return new Lucene9DirectoryReader(directoryReader);
    }

    @Override
    public void commit() throws IOException {
        indexWriter.commit();
    }

    @Override
    public boolean hasCommits() throws IOException {
        Directory directory = indexWriter.getDirectory();
        return DirectoryReader.indexExists(directory) && SegmentInfos.readLatestCommit(directory) != null;
    }

    @Override
    public void forceMerge(int maxNumSegments) throws IOException {
        indexWriter.forceMerge(maxNumSegments);
    }

    @Override
    public void maybeMerge() throws IOException {
        indexWriter.maybeMerge();
    }

    @Override
    public void updateMergePolicy(int mergeFactor, double segmentsPerTier, int maxMergeAtOnce) {
        MergePolicy mergePolicy = indexWriter.getConfig().getMergePolicy();
        switch (mergePolicy) {
            case LogMergePolicy logMergePolicy -> logMergePolicy.setMergeFactor(mergeFactor);
            case TieredMergePolicy tieredMergePolicy -> {
                tieredMergePolicy.setSegmentsPerTier(segmentsPerTier);
                tieredMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
            }
            default -> throw new UnsupportedOperationException("Unexpected mergePolicy: " + mergePolicy);
        }
    }

    @Override
    public void markAsOnline() {
        indexWriter.setLiveCommitData(ONLINE_COMMIT_USER_DATA);
    }

    @Override
    public void addDocuments(Iterable<LuceneDocument> documents) throws IOException {
        indexWriter.addDocuments(convertDocuments(documents));
    }

    @Override
    public int getMaxDocs() {
        return indexWriter.getDocStats().maxDoc;
    }

    @Override
    public void deleteDocuments(String idField, long id) throws IOException {
        indexWriter.deleteDocuments(new Term(idField, Long.toString(id)));
    }

    @Override
    public void addIndexes(LuceneDirectory directory) throws IOException {
        indexWriter.addIndexes(((Lucene9Directory) directory).directory);
    }

    @Override
    public ResourceIterator<Path> snapshot(Path indexFolder) throws IOException {
        IndexDeletionPolicy deletionPolicy = indexWriter.getConfig().getIndexDeletionPolicy();
        if (deletionPolicy instanceof SnapshotDeletionPolicy snapshotDeletionPolicy) {
            if (!hasCommits()) {
                return emptyResourceIterator();
            }
            return new WritableIndexSnapshotFileIterator(indexFolder, snapshotDeletionPolicy);
        } else {
            throw new UnsupportedIndexDeletionPolicy(
                    "Can't perform index snapshot with specified index deletion " + "policy: "
                            + deletionPolicy.toString() + ". " + "Only "
                            + SnapshotDeletionPolicy.class.getName() + " is " + "supported");
        }
    }

    @Override
    public LuceneDocumentsFactory documentFactory() {
        return Lucene9DocumentsFactory.INSTANCE;
    }

    @Override
    public void addDocument(LuceneDocument document) throws IOException {
        indexWriter.addDocument(toInternalDocument(document));
    }

    @Override
    public void close() throws IOException {
        indexWriter.close();
    }

    private static Document toInternalDocument(LuceneDocument document) {
        return ((Lucene9Document) document).document;
    }

    private static Iterable<Document> convertDocuments(Iterable<LuceneDocument> documents) {
        return new DocumentIterable(documents.iterator());
    }

    private static class DocumentIterable implements Iterable<Document> {
        private final Iterator<LuceneDocument> iterator;

        public DocumentIterable(Iterator<LuceneDocument> iterator) {
            this.iterator = iterator;
        }

        @Override
        public Iterator<Document> iterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Document next() {
                    return toInternalDocument(iterator.next());
                }
            };
        }
    }

    /**
     * Iterator over Lucene index files for a particular {@link IndexCommit snapshot}.
     * Applicable only to a single Lucene index partition.
     */
    private static class WritableIndexSnapshotFileIterator extends ReadOnlyIndexSnapshotFileIterator {
        private final SnapshotDeletionPolicy snapshotDeletionPolicy;
        private final IndexCommit indexCommit;

        WritableIndexSnapshotFileIterator(Path indexDirectory, SnapshotDeletionPolicy snapshotDeletionPolicy)
                throws IOException {
            this(indexDirectory, snapshotDeletionPolicy, snapshotDeletionPolicy.snapshot());
        }

        private WritableIndexSnapshotFileIterator(
                Path indexDirectory, SnapshotDeletionPolicy snapshotDeletionPolicy, IndexCommit snapshot)
                throws IOException {
            super(indexDirectory, snapshot.getFileNames());
            this.snapshotDeletionPolicy = snapshotDeletionPolicy;
            this.indexCommit = snapshot;
        }

        @Override
        public void close() {
            try {
                snapshotDeletionPolicy.release(indexCommit);
            } catch (IOException e) {
                throw new SnapshotReleaseException(
                        "Unable to release lucene index snapshot for index in: " + getIndexDirectory(), e);
            }
        }
    }
}
