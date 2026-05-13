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
package org.neo4j.kernel.api.impl.schema.fulltext;

import static org.neo4j.kernel.api.impl.schema.fulltext.LuceneFulltextDocumentStructure.FIELD_ENTITY_ID;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import org.apache.lucene.index.Term;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.collection.BloomFilter;
import org.neo4j.collection.ConcurrentLongBloomFilter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.schema.fulltext.FulltextIndexPopulator.IdLockManager.Lock;
import org.neo4j.kernel.api.impl.schema.populator.LuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

public class FulltextIndexPopulator extends LuceneIndexPopulator<DatabaseIndex<FulltextIndexReader>> {
    private final IndexDescriptor descriptor;
    private final String[] propertyNames;

    // The filter should be effective if not a lot more than EXPECTED_CONCURRENT_UPDATES occur,
    // while the index in question is being populated.
    // We trade a bigger filter for fewer hashes to cut the cost of checks in the populate path
    private static final int EXPECTED_CONCURRENT_UPDATES = 50000;
    private final BloomFilter concurrentUpdateFilter = new ConcurrentLongBloomFilter(EXPECTED_CONCURRENT_UPDATES, 4);
    private final IdLockManager concurrentUpdateLockManager = new IdLockManager();

    FulltextIndexPopulator(
            IndexDescriptor descriptor,
            DatabaseIndex<FulltextIndexReader> luceneFulltext,
            String[] propertyNames,
            IndexUpdateIgnoreStrategy ignoreStrategy) {
        super(luceneFulltext, ignoreStrategy);
        this.descriptor = descriptor;
        this.propertyNames = propertyNames;
    }

    @Override
    public void add(Collection<? extends IndexEntryUpdate> updates, CursorContext cursorContext) {
        try {
            for (IndexEntryUpdate update : updates) {
                ValueIndexEntryUpdate valueUpdate = (ValueIndexEntryUpdate) update;
                if (ignoreStrategy.ignore(valueUpdate.values())) {
                    continue;
                }

                long entityId = valueUpdate.getEntityId();
                LuceneDocument document = updateAsDocument(valueUpdate);
                try (Lock lock = concurrentUpdateLockManager.lock(entityId)) {
                    if (concurrentUpdateFilter.mayContain(entityId)) {
                        writer.updateOrDeleteDocument(FIELD_ENTITY_ID, entityId, document);
                    } else {
                        writer.addDocument(document);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        return new PopulatingFulltextIndexUpdater();
    }

    @Override
    public IndexSample sample(CursorContext cursorContext) {
        return new IndexSample();
    }

    @Override
    public Map<String, Value> indexConfig() {
        return descriptor.getIndexConfig().asMap();
    }

    @Override
    protected LuceneDocument updateAsDocument(ValueIndexEntryUpdate update) {
        Value[] values = update.values();
        return documentsFactory.reusableFulltextDocument(update.getEntityId(), propertyNames, values);
    }

    private class PopulatingFulltextIndexUpdater implements IndexUpdater {
        @Override
        public void process(IndexEntryUpdate update) {
            ValueIndexEntryUpdate valueUpdate = asValueUpdate(update);
            if (ignoreStrategy.ignore(valueUpdate)) {
                return;
            }
            try {
                long entityId = valueUpdate.getEntityId();
                try (Lock lock = concurrentUpdateLockManager.lock(entityId)) {
                    switch (valueUpdate.updateMode()) {
                        case ADDED, CHANGED -> {
                            concurrentUpdateFilter.add(entityId);
                            Value[] values = valueUpdate.values();
                            luceneIndex
                                    .getIndexWriter()
                                    .updateOrDeleteDocument(
                                            FIELD_ENTITY_ID,
                                            entityId,
                                            documentsFactory.reusableFulltextDocument(entityId, propertyNames, values));
                        }
                        case REMOVED -> luceneIndex.getIndexWriter().deleteDocuments(FIELD_ENTITY_ID, entityId);
                        default -> throw new UnsupportedOperationException();
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {}
    }

    /**
     * Specific non-re-entrant lock manager for managing concurrent access between populator threads
     * ({@link  FulltextIndexPopulator#add(Collection, CursorContext)}) and populating updater threads
     * ({@link PopulatingFulltextIndexUpdater#process(IndexEntryUpdate)}).
     *
     * The populator tries to optimize for adding entities which have not been added with the concurrent
     * updater. It knows that it can use {@link org.apache.lucene.index.IndexWriter#addDocument(Iterable)}
     * rather than {@link org.apache.lucene.index.IndexWriter#updateDocument(Term, Iterable)} in the case
     * where the document has not been seen before.
     *
     * Therefore the populator locks the entity id of the document using this lock manager, and if the id has
     * not been seen before (checked using a bloom filter) it writes using {code addDocument()}.
     * The updater locks the entity id before it updates the document, then records the id in the bloom filter
     * so that if the entity id is processed later by the populator, that uses {@code updateDocument}
     */
    static class IdLockManager {

        private final MutableLongSet locked = new LongHashSet();

        private Lock lock(long id) {
            try {
                synchronized (locked) {
                    while (locked.contains(id)) {
                        locked.wait();
                    }
                    locked.add(id);
                }
                return new Lock(id);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        class Lock implements AutoCloseable {

            private final long id;

            private Lock(long id) {
                this.id = id;
            }

            private void unlock() {
                synchronized (locked) {
                    locked.remove(id);
                    locked.notifyAll();
                }
            }

            @Override
            public void close() {
                unlock();
            }
        }
    }
}
