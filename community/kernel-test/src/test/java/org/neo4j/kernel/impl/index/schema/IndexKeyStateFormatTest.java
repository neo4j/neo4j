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
package org.neo4j.kernel.impl.index.schema;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.test.FormatCompatibilityVerifier;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class IndexKeyStateFormatTest<KEY extends NativeIndexKey<KEY>> extends FormatCompatibilityVerifier {
    protected static final int ENTITY_ID = 19570320;

    protected List<Value> values;

    @BeforeEach
    public void setup() {
        values = new ArrayList<>();
        populateValues(values);
    }

    /**
     * Implementing test is responsible for deciding what values should be tested.
     */
    abstract void populateValues(List<Value> values);

    /**
     * Layout that creates the index key.
     * Note that the layout is also tested to some extent.
     */
    abstract Layout<KEY, ?> getLayout();

    /**
     * Get detailed description of the key
     */
    abstract String toDetailedString(KEY key);

    abstract ImmutableSet<OpenOption> getOpenOptions();

    @Override
    protected void createStoreFile(Path storeFile) throws IOException {
        withCursor(storeFile, true, c -> {
            putFormatVersion(c);
            putData(c);
        });
    }

    @Override
    protected void verifyFormat(Path storeFile) throws FormatViolationException, IOException {
        AtomicReference<FormatViolationException> exception = new AtomicReference<>();
        withCursor(storeFile, false, c -> {
            int major = c.getInt();
            int minor = c.getInt();
            Layout<KEY, ?> layout = getLayout();
            if (major != layout.majorVersion() || minor != layout.minorVersion()) {
                exception.set(new FormatViolationException(String.format(
                        "Read format version %d.%d, but layout has version %d.%d",
                        major, minor, layout.majorVersion(), layout.minorVersion())));
            }
        });
        if (exception.get() != null) {
            throw exception.get();
        }
    }

    @Override
    protected void verifyContent(Path storeFile) throws IOException {
        withCursor(storeFile, false, c -> {
            readFormatVersion(c);
            verifyData(c);
        });
    }

    protected void initializeFromValue(KEY key, Value value) {
        key.initialize(ENTITY_ID);
        for (int i = 0; i < GenericKeyStateFormatTest.NUMBER_OF_SLOTS; i++) {
            key.initFromValue(i, value, NativeIndexKey.Inclusion.NEUTRAL);
        }
    }

    protected String detailedFailureMessage(KEY actualKey, KEY expectedKey) {
        return "expected " + toDetailedString(expectedKey) + ", but was " + toDetailedString(actualKey);
    }

    private void withCursor(Path storeFile, boolean create, Consumer<PageCursor> cursorConsumer) throws IOException {
        var openOptions = getOpenOptions().newWith(WRITE);
        if (create) {
            openOptions = openOptions.newWith(CREATE);
        }
        try (PageCache pageCache = PageCacheSupportExtension.getPageCache(globalFs, config());
                PagedFile pagedFile =
                        pageCache.map(storeFile, pageCache.pageSize(), DEFAULT_DATABASE_NAME, openOptions);
                PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, CursorContext.NULL_CONTEXT)) {
            cursor.next();
            cursorConsumer.accept(cursor);
        }
    }

    private void putFormatVersion(PageCursor cursor) {
        Layout<KEY, ?> layout = getLayout();
        int major = layout.majorVersion();
        cursor.putInt(major);
        int minor = layout.minorVersion();
        cursor.putInt(minor);
    }

    private void putData(PageCursor c) {
        Layout<KEY, ?> layout = getLayout();
        KEY key = layout.newKey();
        for (Value value : values) {
            initializeFromValue(key, value);
            c.putInt(layout.keySize(key));
            layout.writeKey(c, key);
        }
    }

    private void verifyData(PageCursor c) {
        Layout<KEY, ?> layout = getLayout();
        KEY readCompositeKey = layout.newKey();
        KEY comparison = layout.newKey();
        for (Value value : values) {
            int keySize = c.getInt();
            layout.readKey(c, readCompositeKey, keySize);
            for (Value readValue : readCompositeKey.asValues()) {
                initializeFromValue(comparison, value);
                assertEquals(
                        0,
                        layout.compare(readCompositeKey, comparison),
                        detailedFailureMessage(readCompositeKey, comparison));
                if (readValue != Values.NO_VALUE) {
                    assertEquals(value, readValue, "expected read value to be " + value + ", but was " + readValue);
                }
            }
        }
    }

    private static void readFormatVersion(PageCursor c) {
        c.getInt(); // Major version
        c.getInt(); // Minor version
    }
}
