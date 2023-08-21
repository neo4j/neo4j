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
package org.neo4j.index.internal.gbptree;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.calculatePayloadSize;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.string.UTF8;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
abstract class GBPTreeLargeDynamicKeysITBase {
    private static final Layout<RawBytes, RawBytes> layout = new SimpleByteArrayLayout(false);

    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fileSystem;

    protected abstract PageCache getPageCache();

    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    @Test
    void putSingleKeyLargerThanInlineCap() throws IOException {
        try (GBPTree<RawBytes, RawBytes> tree = createIndex()) {
            int keySize = Math.min(tree.keyValueSizeCap(), tree.inlineKeyValueSizeCap() + 1);
            RawBytes key = key(keySize);
            RawBytes value = value(0);
            try (Writer<RawBytes, RawBytes> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                writer.put(key, value);
            }
            assertFindExact(tree, key, value);
        }
    }

    @Test
    void removeSingleKeyLargerThanInlineCap() throws IOException {
        try (GBPTree<RawBytes, RawBytes> tree = createIndex()) {
            int keySize = Math.min(tree.keyValueSizeCap(), tree.inlineKeyValueSizeCap() + 1);
            RawBytes key = key(keySize);
            RawBytes value = value(0);
            try (Writer<RawBytes, RawBytes> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                writer.put(key, value);
                writer.remove(key);
            }
            assertDontFind(tree, key);
        }
    }

    @Test
    void putSingleKeyOnKeyValueSizeCap() throws IOException {
        try (GBPTree<RawBytes, RawBytes> tree = createIndex()) {
            int keySize = tree.keyValueSizeCap();
            RawBytes key = key(keySize);
            RawBytes value = value(0);
            try (Writer<RawBytes, RawBytes> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                writer.put(key, value);
            }
            assertFindExact(tree, key, value);
        }
    }

    @Test
    void mustThrowWhenPutSingleKeyLargerThanKeyValueSizeCap() throws IOException {
        try (GBPTree<RawBytes, RawBytes> tree = createIndex()) {
            int keySize = tree.keyValueSizeCap() + 1;
            RawBytes key = key(keySize);
            RawBytes value = value(0);
            try (Writer<RawBytes, RawBytes> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                assertThrows(IllegalArgumentException.class, () -> writer.put(key, value));
            }
        }
    }

    @Test
    void putAndRemoveRandomlyDistributedKeys() throws IOException {
        try (GBPTree<RawBytes, RawBytes> tree = createIndex()) {
            int keyValueSizeOverflow = tree.keyValueSizeCap() + 1;

            RawBytes value = value(0);
            List<Pair<RawBytes, RawBytes>> entries = new ArrayList<>();
            for (int i = 0; i < 2_000; i++) {
                int keySize = inValidRange(4, keyValueSizeOverflow, random.nextInt(keyValueSizeOverflow));
                entries.add(Pair.of(key(keySize, asBytes(i)), value));
            }
            Collections.shuffle(entries, random.random());
            insertAndValidate(tree, entries);
            consistencyCheckStrict(tree);
            removeAndValidate(tree, entries);
            consistencyCheckStrict(tree);
        }
    }

    @Test
    void mustStayCorrectWhenInsertingValuesOfIncreasingLength() throws IOException {
        mustStayCorrectWhenInsertingValuesOfIncreasingLength(false);
    }

    @Test
    void mustStayCorrectWhenInsertingValuesOfIncreasingLengthInRandomOrder() throws IOException {
        mustStayCorrectWhenInsertingValuesOfIncreasingLength(true);
    }

    private void mustStayCorrectWhenInsertingValuesOfIncreasingLength(boolean shuffle) throws IOException {
        try (GBPTree<RawBytes, RawBytes> index = createIndex()) {
            RawBytes emptyValue = layout.newValue();
            emptyValue.bytes = EMPTY_BYTE_ARRAY;
            List<Pair<RawBytes, RawBytes>> entries = new ArrayList<>();
            for (int keySize = 1; keySize < index.keyValueSizeCap(); keySize++) {
                entries.add(Pair.of(key(keySize), emptyValue));
            }
            if (shuffle) {
                Collections.shuffle(entries, random.random());
            }

            insertAndValidate(index, entries);
            consistencyCheckStrict(index);
        }
    }

    @Test
    void shouldWriteAndReadSmallToSemiLargeEntries() throws IOException {
        int pageSize = calculatePayloadSize(getPageCache(), getOpenOptions());
        int keyValueSizeCap = DynamicSizeUtil.keyValueSizeCapFromPageSize(pageSize);
        int minValueSize = 0;
        int maxValueSize = random.nextInt(200);
        int minKeySize = 4;
        int maxKeySize = keyValueSizeCap / 5;
        shouldWriteAndReadEntriesOfRandomSizes(minKeySize, maxKeySize, minValueSize, maxValueSize);
    }

    @Test
    void shouldWriteAndReadSmallToLargeEntries() throws IOException {
        int pageSize = calculatePayloadSize(getPageCache(), getOpenOptions());
        int keyValueSizeCap = DynamicSizeUtil.keyValueSizeCapFromPageSize(pageSize);
        int minValueSize = 0;
        int maxValueSize = random.nextInt(200);
        int minKeySize = 4;
        int maxKeySize = keyValueSizeCap - maxValueSize;
        shouldWriteAndReadEntriesOfRandomSizes(minKeySize, maxKeySize, minValueSize, maxValueSize);
    }

    @Test
    void shouldWriteAndReadSemiLargeToLargeEntries() throws IOException {
        int pageSize = calculatePayloadSize(getPageCache(), getOpenOptions());
        int keyValueSizeCap = DynamicSizeUtil.keyValueSizeCapFromPageSize(pageSize);
        int minValueSize = 0;
        int maxValueSize = random.nextInt(200);
        int minKeySize = keyValueSizeCap / 5;
        int maxKeySize = keyValueSizeCap - maxValueSize;
        shouldWriteAndReadEntriesOfRandomSizes(minKeySize, maxKeySize, minValueSize, maxValueSize);
    }

    private void shouldWriteAndReadEntriesOfRandomSizes(
            int minKeySize, int maxKeySize, int minValueSize, int maxValueSize) throws IOException {
        // given
        try (GBPTree<RawBytes, RawBytes> tree = createIndex()) {
            // when
            Set<String> generatedStrings = new HashSet<>();
            List<Pair<RawBytes, RawBytes>> entries = new ArrayList<>();
            for (int i = 0; i < 1_000; i++) {
                // value, based on i
                RawBytes value = new RawBytes(new byte[random.nextInt(minValueSize, maxValueSize + 1)]);
                random.nextBytes(value.bytes);

                // key, randomly generated
                String string;
                do {
                    string = random.nextAlphaNumericString(minKeySize, maxKeySize);
                } while (!generatedStrings.add(string));
                RawBytes key = new RawBytes(UTF8.encode(string));
                entries.add(Pair.of(key, value));
            }

            insertAndValidate(tree, entries);
            consistencyCheckStrict(tree);
        }
    }

    private void insertAndValidate(GBPTree<RawBytes, RawBytes> tree, List<Pair<RawBytes, RawBytes>> entries)
            throws IOException {
        processWithCheckpoints(tree, entries, (writer, entry) -> writer.put(entry.first(), entry.other()));

        for (Pair<RawBytes, RawBytes> entry : entries) {
            assertFindExact(tree, entry.first(), entry.other());
        }
    }

    private void removeAndValidate(GBPTree<RawBytes, RawBytes> tree, List<Pair<RawBytes, RawBytes>> entries)
            throws IOException {
        processWithCheckpoints(tree, entries, (writer, entry) -> {
            RawBytes removed = writer.remove(entry.first());
            assertEquals(0, layout.compare(removed, entry.other()));
        });

        for (Pair<RawBytes, RawBytes> entry : entries) {
            assertDontFind(tree, entry.first());
        }
    }

    private void processWithCheckpoints(
            GBPTree<RawBytes, RawBytes> tree,
            List<Pair<RawBytes, RawBytes>> entries,
            BiConsumer<Writer<RawBytes, RawBytes>, Pair<RawBytes, RawBytes>> writerAction)
            throws IOException {
        double checkpointFrequency = 0.005;
        Iterator<Pair<RawBytes, RawBytes>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            try (Writer<RawBytes, RawBytes> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                while (iterator.hasNext()) {
                    Pair<RawBytes, RawBytes> entry = iterator.next();
                    writerAction.accept(writer, entry);
                    if (random.nextDouble() < checkpointFrequency) {
                        break;
                    }
                }
            }
            tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }
    }

    private static void assertDontFind(GBPTree<RawBytes, RawBytes> tree, RawBytes key) throws IOException {
        try (Seeker<RawBytes, RawBytes> seek = tree.seek(key, key, NULL_CONTEXT)) {
            assertFalse(seek.next());
        }
    }

    private static void assertFindExact(GBPTree<RawBytes, RawBytes> tree, RawBytes key, RawBytes value)
            throws IOException {
        try (Seeker<RawBytes, RawBytes> seek = tree.seek(key, key, NULL_CONTEXT)) {
            assertTrue(seek.next());
            assertEquals(0, layout.compare(key, seek.key()));
            assertEquals(0, layout.compare(value, seek.value()));
            assertFalse(seek.next());
        }
    }

    private GBPTree<RawBytes, RawBytes> createIndex() {
        // some random padding
        return new GBPTreeBuilder<>(getPageCache(), fileSystem, testDirectory.file("index"), layout)
                .with(getOpenOptions())
                .build();
    }

    private static byte[] asBytes(int value) {
        byte[] intBytes = new byte[Integer.BYTES];
        for (int i = 0, j = intBytes.length - 1; i < intBytes.length; i++, j--) {
            intBytes[j] = (byte) (value >>> i * Byte.SIZE);
        }
        return intBytes;
    }

    private static RawBytes key(int keySize, byte... firstBytes) {
        RawBytes key = layout.newKey();
        key.bytes = new byte[keySize];
        for (int i = 0; i < firstBytes.length && i < keySize; i++) {
            key.bytes[i] = firstBytes[i];
        }
        return key;
    }

    private static RawBytes value(int valueSize) {
        RawBytes value = layout.newValue();
        value.bytes = new byte[valueSize];
        return value;
    }

    private static int inValidRange(int min, int max, int value) {
        return Math.min(max, Math.max(min, value));
    }
}
