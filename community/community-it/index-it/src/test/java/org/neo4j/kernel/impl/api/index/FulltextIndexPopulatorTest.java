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
package org.neo4j.kernel.impl.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.schema.fulltext.LuceneFulltextDocumentStructure.FIELD_ENTITY_ID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10DocumentsFactory;
import org.neo4j.kernel.api.impl.schema.fulltext.FulltextIndexPopulator;
import org.neo4j.kernel.api.impl.schema.fulltext.FulltextIndexPopulatorTestExtension;
import org.neo4j.kernel.api.impl.schema.fulltext.FulltextIndexReader;
import org.neo4j.kernel.api.impl.schema.writer.LucenePartitionIndexWriter;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith({RandomExtension.class})
public class FulltextIndexPopulatorTest {

    @Inject
    private RandomSupport random;

    private IndexDescriptor indexDescriptor;
    private SchemaDescriptor schemaDescriptor;
    private DatabaseIndex<FulltextIndexReader> luceneFulltext;
    private IndexUpdateIgnoreStrategy indexUpdateIgnoreStrategy;
    private FulltextIndexPopulator fulltextIndexPopulator;
    private CursorContext cursorContext;
    private CapturingIndexWriter indexWriter;
    private LuceneDocumentsFactory documentsFactory;
    private AtomicLong counter;
    private CountDownLatch beforeAddLatch;
    private CountDownLatch proceedAddLatch;
    private CountDownLatch beforeUpdateLatch;
    private CountDownLatch proceedUpdateLatch;

    private static final int[] PROPERTY_IDS = new int[] {101, 102};
    private static final String[] PROPERTY_NAMES = new String[] {"p101", "p102"};

    @BeforeEach
    void before() {
        counter = new AtomicLong(10000);
        cursorContext = mock(CursorContext.class);

        schemaDescriptor = mock(SchemaDescriptor.class);
        when(schemaDescriptor.getPropertyIds()).thenReturn(PROPERTY_IDS);

        indexDescriptor = mock(IndexDescriptor.class);
        when(indexDescriptor.schema()).thenReturn(schemaDescriptor);

        documentsFactory = new Lucene10DocumentsFactory();

        indexWriter = new CapturingIndexWriter();

        //noinspection unchecked
        luceneFulltext = mock(DatabaseIndex.class);
        when(luceneFulltext.getIndexWriter()).thenReturn(indexWriter);

        indexUpdateIgnoreStrategy = mock(IndexUpdateIgnoreStrategy.class);
        when(indexUpdateIgnoreStrategy.ignore((ValueIndexEntryUpdate) any())).thenReturn(false);
        when(indexUpdateIgnoreStrategy.ignore((Value[]) any())).thenReturn(false);

        fulltextIndexPopulator = new FulltextIndexPopulatorTestExtension(
                indexDescriptor, luceneFulltext, PROPERTY_NAMES, indexUpdateIgnoreStrategy);
        fulltextIndexPopulator.create();

        beforeAddLatch = new CountDownLatch(1);
        proceedAddLatch = new CountDownLatch(1);
        beforeUpdateLatch = new CountDownLatch(1);
        proceedUpdateLatch = new CountDownLatch(1);

        // these latches are normally passed right through
        // replace them if you need the add or update to wait
        proceedAddLatch.countDown();
        proceedUpdateLatch.countDown();
    }

    @Test
    public void scanPopulationShouldBeUpdateAfterConcurrent() throws IndexEntryConflictException {

        EagerValueIndexEntryUpdate fromScan = add("one flew over the cuckoo's nest", "one un uno");
        EagerValueIndexEntryUpdate fromTx = change(fromScan, "one flew over the cuckoo's nest", "two deux due");
        fulltextIndexPopulator.newPopulatingUpdater(cursorContext).process(fromTx);
        fulltextIndexPopulator.add(List.of(fromScan), cursorContext);

        Written first = indexWriter.next();
        assertThat(first.writeType).isEqualTo(WriteType.Update);
        assertThat(first.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromTx));

        Written second = indexWriter.next();
        assertThat(second.writeType).isEqualTo(WriteType.Update);
        assertThat(second.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromScan));

        assertThat(indexWriter.hasNext()).isFalse();
    }

    @Test
    public void scanPopulationShouldBeAddBeforeConcurrent() throws IndexEntryConflictException {

        EagerValueIndexEntryUpdate fromScan = add("one flew over the cuckoo's nest", "one un uno");
        EagerValueIndexEntryUpdate fromTx = change(fromScan, "one flew over the cuckoo's nest", "two deux due");
        fulltextIndexPopulator.add(List.of(fromScan), cursorContext);
        fulltextIndexPopulator.newPopulatingUpdater(cursorContext).process(fromTx);

        Written first = indexWriter.next();
        assertThat(first.writeType).isEqualTo(WriteType.Add);
        assertThat(first.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromScan));

        Written second = indexWriter.next();
        assertThat(second.writeType).isEqualTo(WriteType.Update);
        assertThat(second.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromTx));

        assertThat(indexWriter.hasNext()).isFalse();
    }

    @Test
    public void scanPopulationShouldBeAddIfIndependent() throws IndexEntryConflictException {

        EagerValueIndexEntryUpdate fromScan = add("one flew over the cuckoo's nest", "one un uno");
        EagerValueIndexEntryUpdate fromTx = add("Slaughterhouse Five", "two deux due");
        fulltextIndexPopulator.newPopulatingUpdater(cursorContext).process(fromTx);
        fulltextIndexPopulator.add(List.of(fromScan), cursorContext);

        Written first = indexWriter.next();
        assertThat(first.writeType).isEqualTo(WriteType.Update);
        assertThat(first.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromTx));

        Written second = indexWriter.next();
        assertThat(second.writeType).isEqualTo(WriteType.Add);
        assertThat(second.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromScan));

        assertThat(indexWriter.hasNext()).isFalse();
    }

    @Test
    public void scanPopulationShouldLockBlockOnConcurrentThenUpdate() throws InterruptedException, ExecutionException {

        EagerValueIndexEntryUpdate fromScan = add("one flew over the cuckoo's nest", "one un uno");
        EagerValueIndexEntryUpdate fromTx = change(fromScan, "one flew over the cuckoo's nest", "two deux due");

        beforeUpdateLatch = new CountDownLatch(1);
        proceedUpdateLatch = new CountDownLatch(1);
        Future<?> future1 = onThread(
                () -> fulltextIndexPopulator.newPopulatingUpdater(cursorContext).process(fromTx));
        beforeUpdateLatch.await();
        // fromTx holds its lock, is waiting for proceedUpdateLatch

        beforeUpdateLatch = new CountDownLatch(1);
        Future<?> future2 = onThread(() -> fulltextIndexPopulator.add(List.of(fromScan), cursorContext));
        Thread.sleep(100);
        proceedUpdateLatch.countDown();

        // replace latch for fromScan
        proceedUpdateLatch = new CountDownLatch(1);
        beforeUpdateLatch.await();
        proceedUpdateLatch.countDown();

        future1.get();
        future2.get();

        Written first = indexWriter.next();
        assertThat(first.writeType).isEqualTo(WriteType.Update);
        assertThat(first.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromTx));

        Written second = indexWriter.next();
        assertThat(second.writeType).isEqualTo(WriteType.Update);
        assertThat(second.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromScan));

        assertThat(indexWriter.hasNext()).isFalse();
    }

    @Test
    public void scanPopulationIndependentLocksShouldNoBlockAdd() throws InterruptedException, ExecutionException {

        EagerValueIndexEntryUpdate fromScan = add("one flew over the cuckoo's nest", "one un uno");
        EagerValueIndexEntryUpdate fromTx = add("Slaughterhouse Five", "two deux due");

        beforeUpdateLatch = new CountDownLatch(1);
        proceedUpdateLatch = new CountDownLatch(1);
        Future<?> future1 = onThread(
                () -> fulltextIndexPopulator.newPopulatingUpdater(cursorContext).process(fromTx));
        beforeUpdateLatch.await();
        // fromTx holds its lock, is waiting for proceedUpdateLatch

        beforeAddLatch = new CountDownLatch(1);
        proceedAddLatch = new CountDownLatch(1);
        Future<?> future2 = onThread(() -> fulltextIndexPopulator.add(List.of(fromScan), cursorContext));

        beforeAddLatch.await();
        proceedAddLatch.countDown();

        future2.get();
        proceedUpdateLatch.countDown();

        future1.get();

        Written first = indexWriter.next();
        assertThat(first.writeType).isEqualTo(WriteType.Add);
        assertThat(first.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromScan));

        Written second = indexWriter.next();
        assertThat(second.writeType).isEqualTo(WriteType.Update);
        assertThat(second.values()).containsExactlyInAnyOrderEntriesOf(entriesOf(fromTx));

        assertThat(indexWriter.hasNext()).isFalse();
    }

    private EagerValueIndexEntryUpdate add(long entityId, Object... values) {
        return EagerValueIndexEntryUpdate.add(
                entityId, indexDescriptor, Stream.of(values).map(Values::of).toArray(Value[]::new));
    }

    private EagerValueIndexEntryUpdate add(Object... values) {
        return add(counter.getAndIncrement(), values);
    }

    private EagerValueIndexEntryUpdate change(IndexEntryUpdate previous, Object... values) {
        return add(previous.getEntityId(), values);
    }

    private record Written(WriteType writeType, long entityId, Map<String, Object> values) {
        static Written of(WriteType writeType, long entityId, LuceneDocument document) {
            Map<String, Object> properties = new HashMap<>();
            for (String propertyName : PROPERTY_NAMES) {
                properties.put(propertyName, document.get(propertyName));
            }
            return new Written(writeType, entityId, properties);
        }
    }

    private enum WriteType {
        Update,
        Add
    }

    private class CapturingIndexWriter implements LucenePartitionIndexWriter {

        final List<Written> written = new ArrayList<>();
        final LuceneDocumentsFactory documentsFactory = new Lucene10DocumentsFactory();

        @Override
        public LuceneDocumentsFactory documentsFactory() {
            return documentsFactory;
        }

        @Override
        public void addDocument(LuceneDocument document) {
            beforeAddLatch.countDown();
            try {
                proceedAddLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            long id = Long.parseLong(document.get(FIELD_ENTITY_ID));
            written.add(Written.of(WriteType.Add, id, document));
        }

        @Override
        public void addDocuments(int numDocs, Iterable<LuceneDocument> documents) {
            beforeAddLatch.countDown();
            try {
                proceedAddLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (LuceneDocument document : documents) {
                addDocument(document);
            }
        }

        @Override
        public void updateDocument(String idField, long id, LuceneDocument document) {
            beforeUpdateLatch.countDown();
            try {
                proceedUpdateLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            written.add(Written.of(WriteType.Update, id, document));
        }

        @Override
        public void deleteDocuments(String idField, long id) {
            Assertions.fail("deleteDocuments() called");
        }

        @Override
        public void addDirectory(int count, LuceneDirectory directory) {
            Assertions.fail("addDirectory() called");
        }

        boolean hasNext() {
            return !written.isEmpty();
        }

        Written next() {
            return written.removeFirst();
        }
    }

    private static Map<String, Object> entriesOf(EagerValueIndexEntryUpdate update) {
        Map<String, Object> entries = new HashMap<>();
        int propertyIndex = 0;
        for (Value value : update.values()) {
            entries.put(PROPERTY_NAMES[propertyIndex++], value.asObject());
        }
        return entries;
    }

    @FunctionalInterface
    public interface Throwing<T extends Throwable> {
        void run() throws T;
    }

    private static <T extends Throwable> Future<?> onThread(Throwing<T> runnable) {
        return Executors.newFixedThreadPool(1).submit(() -> {
            try {
                runnable.run();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }
}
