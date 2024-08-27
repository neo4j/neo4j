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
package org.neo4j.kernel.impl.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@Neo4jLayoutExtension
class ShortStringPropertyEncodeTest {
    private static final int KEY_ID = 0;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    private NeoStores neoStores;
    private PropertyStore propertyStore;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void setupStore() {
        var pageCacheTracer = PageCacheTracer.NULL;
        neoStores = new StoreFactory(
                        databaseLayout,
                        Config.defaults(),
                        new DefaultIdGeneratorFactory(
                                fileSystem, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                        pageCache,
                        pageCacheTracer,
                        fileSystem,
                        NullLogProvider.getInstance(),
                        new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL)
                .openNeoStores(StoreType.PROPERTY, StoreType.PROPERTY_ARRAY, StoreType.PROPERTY_STRING);
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        propertyStore = neoStores.getPropertyStore();
    }

    @AfterEach
    void closeStore() {
        neoStores.close();
    }

    @Test
    void canEncodeEmptyString() {
        assertCanEncode("");
    }

    @Test
    void canEncodeReallyLongString() {
        assertCanEncode("                    "); // 20 spaces
        assertCanEncode("                "); // 16 spaces
    }

    @Test
    void canEncodeFifteenSpaces() {
        assertCanEncode("               ");
    }

    @Test
    void canEncodeNumericalString() {
        assertCanEncode("0123456789+,'.-");
        assertCanEncode(" ,'.-0123456789");
        assertCanEncode("+ '.0123456789-");
        assertCanEncode("+, 0123456789.-");
        assertCanEncode("+,0123456789' -");
        assertCanEncode("+0123456789,'. ");
        // IP(v4) numbers
        assertCanEncode("192.168.0.1");
        assertCanEncode("127.0.0.1");
        assertCanEncode("255.255.255.255");
    }

    @Test
    void canEncodeTooLongStringsWithCharsInDifferentTables() {
        assertCanEncode("____________+");
        assertCanEncode("_____+_____");
        assertCanEncode("____+____");
        assertCanEncode("HELLO world");
        assertCanEncode("Hello_World");
    }

    @Test
    void canEncodeUpToNineEuropeanChars() {
        // Shorter than 10 chars
        assertCanEncode("fågel"); // "bird" in Swedish
        assertCanEncode("påfågel"); // "peacock" in Swedish
        assertCanEncode("påfågelö"); // "peacock island" in Swedish
        assertCanEncode("påfågelön"); // "the peacock island" in Swedish
        // 10 chars
        assertCanEncode("påfågelöar"); // "peacock islands" in Swedish
    }

    @Test
    void canEncodeEuropeanCharsWithPunctuation() {
        assertCanEncode("qHm7 pp3");
        assertCanEncode("UKKY3t.gk");
    }

    @Test
    void canEncodeAlphanumerical() {
        assertCanEncode("1234567890"); // Just a sanity check
        assertCanEncodeInBothCasings("HelloWor1d"); // There is a number there
        assertCanEncode("          "); // Alphanum is the first that can encode 10 spaces
        assertCanEncode("_ _ _ _ _ "); // The only available punctuation
        assertCanEncode("H3Lo_ or1D"); // Mixed case + punctuation
        assertCanEncode("q1w2e3r4t+"); // + is not in the charset
    }

    @Test
    void canEncodeHighUnicode() {
        assertCanEncode("\u02FF");
        assertCanEncode("hello\u02FF");
    }

    @Test
    void canEncodeLatin1SpecialChars() {
        assertCanEncode("#$#$#$#");
        assertCanEncode("$hello#");
    }

    @Test
    void canEncodeTooLongLatin1String() {
        assertCanEncode("#$#$#$#$");
    }

    @Test
    void canEncodeLowercaseAndUppercaseStringsUpTo12Chars() {
        assertCanEncodeInBothCasings("hello world");
        assertCanEncode("hello_world");
        assertCanEncode("_hello_world");
        assertCanEncode("hello::world");
        assertCanEncode("hello//world");
        assertCanEncode("hello world");
        assertCanEncode("http://ok");
        assertCanEncode("::::::::");
        assertCanEncode(" _.-:/ _.-:/");
    }

    private void assertCanEncodeInBothCasings(String string) {
        assertCanEncode(string.toLowerCase());
        assertCanEncode(string.toUpperCase());
    }

    private void assertCanEncode(String string) {
        encode(string);
    }

    private void encode(String string) {
        PropertyBlock block = new PropertyBlock();
        TextValue expectedValue = Values.stringValue(string);
        PropertyStore.encodeValue(
                block,
                KEY_ID,
                expectedValue,
                allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                CursorContext.NULL_CONTEXT,
                INSTANCE);
        assertEquals(0, block.getValueRecords().size());
        Value readValue = block.getType().value(block, propertyStore, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);
        assertEquals(expectedValue, readValue);
    }
}
