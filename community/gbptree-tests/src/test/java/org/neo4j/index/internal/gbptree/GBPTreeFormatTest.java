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

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.test.FormatCompatibilityVerifier;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.tags.MultiVersionedTag;
import org.neo4j.test.utils.PageCacheConfig;

@ExtendWith(RandomExtension.class)
public class GBPTreeFormatTest<KEY, VALUE> extends FormatCompatibilityVerifier {

    private static final String STORE = "store";
    private static final int INITIAL_KEY_COUNT = 1_000;
    private static final int PAGE_SIZE_8K = (int) ByteUnit.kibiBytes(8);
    private static final int PAGE_SIZE_16K = (int) ByteUnit.kibiBytes(16);
    private static final int PAGE_SIZE_32K = (int) ByteUnit.kibiBytes(32);
    private static final int PAGE_SIZE_64K = (int) ByteUnit.kibiBytes(64);
    private static final int PAGE_SIZE_4M = (int) ByteUnit.mebiBytes(4);
    private static final String CURRENT_FIXED_SIZE_FORMAT_8k_ZIP = "current-format_8k.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_8k_ZIP = "current-dynamic-format_8k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_16k_ZIP = "current-format_16k.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_16k_ZIP = "current-dynamic-format_16k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_32k_ZIP = "current-format_32k.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_32k_ZIP = "current-dynamic-format_32k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_64k_ZIP = "current-format_64k.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_4M_ZIP = "current-format_4M.zip";

    private static final String CURRENT_FIXED_SIZE_FORMAT_8k_LE_ZIP = "current-format_8k_le.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_8k_LE_ZIP = "current-dynamic-format_8k_le.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_16k_LE_ZIP = "current-format_16k_le.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_16k_LE_ZIP = "current-dynamic-format_16k_le.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_32k_LE_ZIP = "current-format_32k_le.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_32k_LE_ZIP = "current-dynamic-format_32k_le.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_64k_LE_ZIP = "current-format_64k_le.zip";
    private static final String CURRENT_FIXED_SIZE_FORMAT_4M_LE_ZIP = "current-format_4M_le.zip";

    private TestLayout<KEY, VALUE> layout;
    private String zipName;
    private ImmutableSet<OpenOption> openOptions;

    private static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_8k_ZIP,
                        PAGE_SIZE_8K,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),
                Arguments.of(
                        new SimpleByteArrayLayout(4000, 99),
                        CURRENT_DYNAMIC_SIZE_FORMAT_8k_ZIP,
                        PAGE_SIZE_8K,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),
                // 16k
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_16k_ZIP,
                        PAGE_SIZE_16K,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),
                Arguments.of(
                        new SimpleByteArrayLayout(4000, 99),
                        CURRENT_DYNAMIC_SIZE_FORMAT_16k_ZIP,
                        PAGE_SIZE_16K,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),
                // 32k
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_32k_ZIP,
                        PAGE_SIZE_32K,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),
                Arguments.of(
                        new SimpleByteArrayLayout(4000, 99),
                        CURRENT_DYNAMIC_SIZE_FORMAT_32k_ZIP,
                        PAGE_SIZE_32K,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),
                // 64k
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_64k_ZIP,
                        PAGE_SIZE_64K,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),
                // 4M
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_4M_ZIP,
                        PAGE_SIZE_4M,
                        immutable.of(PageCacheOpenOptions.BIG_ENDIAN)),

                // Same but little-endian now
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_8k_LE_ZIP,
                        PAGE_SIZE_8K,
                        immutable.empty()),
                Arguments.of(
                        new SimpleByteArrayLayout(4000, 99),
                        CURRENT_DYNAMIC_SIZE_FORMAT_8k_LE_ZIP,
                        PAGE_SIZE_8K,
                        immutable.empty()),
                // 16k
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_16k_LE_ZIP,
                        PAGE_SIZE_16K,
                        immutable.empty()),
                Arguments.of(
                        new SimpleByteArrayLayout(4000, 99),
                        CURRENT_DYNAMIC_SIZE_FORMAT_16k_LE_ZIP,
                        PAGE_SIZE_16K,
                        immutable.empty()),
                // 32k
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_32k_LE_ZIP,
                        PAGE_SIZE_32K,
                        immutable.empty()),
                Arguments.of(
                        new SimpleByteArrayLayout(4000, 99),
                        CURRENT_DYNAMIC_SIZE_FORMAT_32k_LE_ZIP,
                        PAGE_SIZE_32K,
                        immutable.empty()),
                // 64k
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_64k_LE_ZIP,
                        PAGE_SIZE_64K,
                        immutable.empty()),
                // 4M
                Arguments.of(
                        longLayout().withFixedSize(true).build(),
                        CURRENT_FIXED_SIZE_FORMAT_4M_LE_ZIP,
                        PAGE_SIZE_4M,
                        immutable.empty()));
    }

    @Override
    public void shouldDetectFormatChange() {
        // Nothing
    }

    @ParameterizedTest
    @MethodSource("data")
    @MultiVersionedTag
    public void shouldDetectFormatChange(
            TestLayout<KEY, VALUE> layout, String zipName, int pageSize, ImmutableSet<OpenOption> openOptions)
            throws Throwable {
        try {
            init(layout, zipName, pageSize, openOptions);
            super.shouldDetectFormatChange();
        } finally {
            clear();
        }
    }

    private void init(
            TestLayout<KEY, VALUE> layout, String zipName, int pageSize, ImmutableSet<OpenOption> openOptions) {
        this.layout = layout;
        this.zipName = zipName;
        this.openOptions = openOptions;

        allKeys = new ArrayList<>();
        allKeys.addAll(initialKeys);
        allKeys.addAll(keysToAdd);
        allKeys.sort(Long::compare);
        PageCacheConfig overriddenConfig = PageCacheConfig.config().withPageSize(pageSize);
        if (pageSize == PAGE_SIZE_4M) {
            overriddenConfig.withMemory("16MiB");
        }
        pageCache = PageCacheSupportExtension.getPageCache(globalFs, overriddenConfig);
    }

    private void clear() {
        if (pageCache != null) {
            pageCache.close();
        }
    }

    @Inject
    private RandomSupport random;

    private final List<Long> initialKeys = initialKeys();
    private final List<Long> keysToAdd = keysToAdd();
    private List<Long> allKeys;
    private PageCache pageCache;

    @Override
    protected String zipName() {
        return zipName;
    }

    @Override
    protected String storeFileName() {
        return STORE;
    }

    @Override
    protected void createStoreFile(Path storeFile) throws IOException {
        List<Long> initialKeys = initialKeys();
        try (GBPTree<KEY, VALUE> tree = new GBPTreeBuilder<>(pageCache, globalFs, storeFile, layout)
                .with(openOptions)
                .build()) {
            try (Writer<KEY, VALUE> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                for (Long key : initialKeys) {
                    put(writer, key);
                }
            }
            tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }
    }

    /**
     * Throws {@link FormatViolationException} if format has changed.
     */
    @SuppressWarnings("EmptyTryBlock")
    @Override
    protected void verifyFormat(Path storeFile) throws IOException, FormatViolationException {
        try (GBPTree<KEY, VALUE> ignored = new GBPTreeBuilder<>(pageCache, globalFs, storeFile, layout)
                .with(openOptions)
                .build()) {
        } catch (MetadataMismatchException e) {
            throw new FormatViolationException(e);
        }
    }

    @Override
    public void verifyContent(Path storeFile) throws IOException {
        try (GBPTree<KEY, VALUE> tree = new GBPTreeBuilder<>(pageCache, globalFs, storeFile, layout)
                .with(openOptions)
                .build()) {
            {
                // WHEN reading from the tree
                // THEN initial keys should be there
                consistencyCheckStrict(tree);
                try (Seeker<KEY, VALUE> cursor = tree.seek(layout.key(0), layout.key(Long.MAX_VALUE), NULL_CONTEXT)) {
                    for (Long expectedKey : initialKeys) {
                        assertHit(cursor, layout, expectedKey);
                    }
                    assertFalse(cursor.next());
                }
            }

            {
                // WHEN writing more to the tree
                // THEN we should not see any format conflicts
                try (Writer<KEY, VALUE> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                    while (keysToAdd.size() > 0) {
                        int next = random.nextInt(keysToAdd.size());
                        Long key = keysToAdd.get(next);
                        put(writer, key);
                        keysToAdd.remove(next);
                    }
                }
            }

            {
                // WHEN reading from the tree again
                // THEN all keys including newly added should be there
                consistencyCheckStrict(tree);
                try (Seeker<KEY, VALUE> cursor =
                        tree.seek(layout.key(0), layout.key(2 * INITIAL_KEY_COUNT), NULL_CONTEXT)) {
                    for (Long expectedKey : allKeys) {
                        assertHit(cursor, layout, expectedKey);
                    }
                    assertFalse(cursor.next());
                }
            }

            {
                // WHEN randomly removing half of tree content
                // THEN we should not see any format conflicts
                try (Writer<KEY, VALUE> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                    int size = allKeys.size();
                    while (allKeys.size() > size / 2) {
                        int next = random.nextInt(allKeys.size());
                        KEY key = layout.key(allKeys.get(next));
                        writer.remove(key);
                        allKeys.remove(next);
                    }
                }
            }

            {
                // WHEN reading from the tree after remove
                // THEN we should see everything that is left in the tree
                consistencyCheckStrict(tree);
                try (Seeker<KEY, VALUE> cursor =
                        tree.seek(layout.key(0), layout.key(2 * INITIAL_KEY_COUNT), NULL_CONTEXT)) {
                    for (Long expectedKey : allKeys) {
                        assertHit(cursor, layout, expectedKey);
                    }
                    assertFalse(cursor.next());
                }
            }
        }
    }

    private static long value(long key) {
        return (long) (key * 1.5);
    }

    private static List<Long> initialKeys() {
        List<Long> initialKeys = new ArrayList<>();
        for (long i = 0, key = 0; i < INITIAL_KEY_COUNT; i++, key += 2) {
            initialKeys.add(key);
        }
        return initialKeys;
    }

    private static List<Long> keysToAdd() {
        List<Long> keysToAdd = new ArrayList<>();
        for (long i = 0, key = 1; i < INITIAL_KEY_COUNT; i++, key += 2) {
            keysToAdd.add(key);
        }
        return keysToAdd;
    }

    private static <KEY, VALUE> void assertHit(
            Seeker<KEY, VALUE> cursor, TestLayout<KEY, VALUE> layout, Long expectedKey) throws IOException {
        assertTrue(cursor.next(), "Had no next when expecting key " + expectedKey);
        assertEquals(expectedKey.longValue(), layout.keySeed(cursor.key()));
        assertEquals(value(expectedKey), layout.valueSeed(cursor.value()));
    }

    private void put(Writer<KEY, VALUE> writer, long key) {
        KEY insertKey = layout.key(key);
        VALUE insertValue = layout.value(value(key));
        writer.put(insertKey, insertValue);
    }
}
