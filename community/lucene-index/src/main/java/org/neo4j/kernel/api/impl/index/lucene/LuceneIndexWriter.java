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
package org.neo4j.kernel.api.impl.index.lucene;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.IndexWriter;
import org.neo4j.graphdb.ResourceIterator;

public interface LuceneIndexWriter extends Closeable {
    int MAX_DOCS = Integer.MAX_VALUE - 128;
    int MAX_TERM_LENGTH = IndexWriter.MAX_TERM_LENGTH;
    String WRITE_LOCK_NAME = IndexWriter.WRITE_LOCK_NAME;

    String KEY_STATUS = "status";
    String ONLINE = "online";
    Set<Map.Entry<String, String>> ONLINE_COMMIT_USER_DATA = Set.of(Map.entry(KEY_STATUS, ONLINE));

    LuceneDocument newDocument();

    void addDocument(LuceneDocument document) throws IOException;

    void addDocuments(Iterable<LuceneDocument> documents) throws IOException;

    void updateDocument(String idField, long id, LuceneDocument document) throws IOException;

    void deleteDocuments(String idField, long id) throws IOException;

    void commit() throws IOException;

    void rollback() throws IOException;

    LuceneDirectoryReader directoryReader() throws IOException;

    boolean hasCommits() throws IOException;

    void forceMerge(int maxNumSegments) throws IOException;

    void maybeMerge() throws IOException;

    /**
     * Re-apply merge-policy parameters to the already-open writer, mutating the live {@link MergePolicy}
     * instance in place (it cannot be replaced on an open writer). Used to swap the population tuning
     * (e.g. {@code LOG_BYTE_SIZED} with a large {@code mergeFactor}) for the standard tuning before a
     * post-population {@link #maybeMerge()}, so the natural merge policy actually consolidates.
     */
    void updateMergePolicy(int mergeFactor, double segmentsPerTier, int maxMergeAtOnce);

    void markAsOnline();

    int getMaxDocs();

    void addIndexes(LuceneDirectory directory) throws IOException;

    ResourceIterator<Path> snapshot(Path indexFolder) throws IOException;

    LuceneDocumentsFactory documentFactory();
}
