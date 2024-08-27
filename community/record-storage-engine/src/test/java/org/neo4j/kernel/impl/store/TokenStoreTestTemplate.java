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

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.api.NamedToken;

@EphemeralPageCacheExtension
abstract class TokenStoreTestTemplate<R extends TokenRecord> {
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    @Inject
    private PageCache pageCache;

    private TokenStore<R> store;
    private DynamicStringStore nameStore;
    protected StoreCursors storeCursors;

    @BeforeEach
    void setUp() throws IOException {
        Path file = dir.file("label-tokens.db");
        Path idFile = dir.file("label-tokens.db.id");
        Path namesFile = dir.file("label-tokens.db.names");
        Path namesIdFile = dir.file("label-tokens.db.names.id");

        IdGeneratorFactory generatorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), PageCacheTracer.NULL, DEFAULT_DATABASE_NAME);
        InternalLogProvider logProvider = NullLogProvider.getInstance();

        RecordFormats formats = RecordFormatSelector.defaultFormat();
        Config config = Config.defaults();
        nameStore = new DynamicStringStore(
                fs,
                namesFile,
                namesIdFile,
                config,
                RecordIdType.LABEL_TOKEN_NAME,
                generatorFactory,
                pageCache,
                PageCacheTracer.NULL,
                logProvider,
                TokenStore.NAME_STORE_BLOCK_SIZE,
                formats.dynamic(),
                false,
                DEFAULT_DATABASE_NAME,
                immutable.empty());
        store = instantiateStore(
                fs, file, idFile, generatorFactory, pageCache, logProvider, nameStore, formats, config);
        CursorContextFactory contextFactory = new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);
        nameStore.initialise(contextFactory);
        store.initialise(contextFactory);
        nameStore.start(NULL_CONTEXT);
        store.start(NULL_CONTEXT);
        storeCursors = createCursors(store, nameStore);
    }

    protected abstract PageCursor storeCursor();

    protected abstract StoreCursors createCursors(TokenStore<R> store, DynamicStringStore nameStore);

    protected abstract TokenStore<R> instantiateStore(
            FileSystemAbstraction fileSystem,
            Path file,
            Path idFile,
            IdGeneratorFactory generatorFactory,
            PageCache pageCache,
            InternalLogProvider logProvider,
            DynamicStringStore nameStore,
            RecordFormats formats,
            Config config);

    @AfterEach
    void tearDown() throws IOException {
        IOUtils.closeAll(store, nameStore);
    }

    @Test
    void forceGetRecordSkipInUseCheck() throws IOException {
        createEmptyPageZero();
        R record = store.getRecordByCursor(7, store.newRecord(), FORCE, storeCursor(), EmptyMemoryTracker.INSTANCE);
        assertFalse(record.inUse(), "Record should not be in use");
    }

    @Test
    void getRecordWithNormalModeMustThrowIfTheRecordIsNotInUse() throws IOException {
        createEmptyPageZero();

        assertThrows(
                InvalidRecordException.class,
                () -> store.getRecordByCursor(
                        7, store.newRecord(), NORMAL, storeCursor(), EmptyMemoryTracker.INSTANCE));
    }

    @Test
    void tokensMustNotBeInternalByDefault() {
        R tokenRecord = createInUseRecord(allocateNameRecords("MyToken"));
        storeToken(tokenRecord);

        R readBack = store.getRecordByCursor(
                tokenRecord.getId(), store.newRecord(), NORMAL, storeCursor(), EmptyMemoryTracker.INSTANCE);
        store.ensureHeavy(readBack, storeCursors, EmptyMemoryTracker.INSTANCE);
        assertThat(readBack).isEqualTo(tokenRecord);
        assertThat(tokenRecord.isInternal()).isEqualTo(false);
        assertThat(readBack.isInternal()).isEqualTo(false);
    }

    @Test
    void tokensMustPreserveTheirInternalFlag() {
        R tokenRecord = createInUseRecord(allocateNameRecords("MyInternalToken"));
        tokenRecord.setInternal(true);
        storeToken(tokenRecord);

        R readBack = store.getRecordByCursor(
                tokenRecord.getId(), store.newRecord(), NORMAL, storeCursor(), EmptyMemoryTracker.INSTANCE);
        store.ensureHeavy(readBack, storeCursors, EmptyMemoryTracker.INSTANCE);
        assertThat(readBack).isEqualTo(tokenRecord);
        assertThat(tokenRecord.isInternal()).isEqualTo(true);
        assertThat(readBack.isInternal()).isEqualTo(true);

        NamedToken token = store.getToken(toIntExact(tokenRecord.getId()), storeCursors, EmptyMemoryTracker.INSTANCE);
        assertThat(token.name()).isEqualTo("MyInternalToken");
        assertThat(token.id()).isIn(toIntExact(tokenRecord.getId()));
        assertTrue(token.isInternal());
    }

    @Test
    void gettingAllReadableTokensAndAllTokensMustAlsoReturnTokensThatAreInternal() {
        R tokenA = createInUseRecord(allocateNameRecords("TokenA"));
        R tokenB = createInUseRecord(allocateNameRecords("TokenB"));
        R tokenC = createInUseRecord(allocateNameRecords("TokenC"));
        tokenC.setInternal(true);
        R tokenD = createInUseRecord(allocateNameRecords("TokenD"));

        storeToken(tokenA);
        storeToken(tokenB);
        storeToken(tokenC);
        storeToken(tokenD);

        R readA = store.getRecordByCursor(
                tokenA.getId(), store.newRecord(), NORMAL, storeCursor(), EmptyMemoryTracker.INSTANCE);
        R readB = store.getRecordByCursor(
                tokenB.getId(), store.newRecord(), NORMAL, storeCursor(), EmptyMemoryTracker.INSTANCE);
        R readC = store.getRecordByCursor(
                tokenC.getId(), store.newRecord(), NORMAL, storeCursor(), EmptyMemoryTracker.INSTANCE);
        R readD = store.getRecordByCursor(
                tokenD.getId(), store.newRecord(), NORMAL, storeCursor(), EmptyMemoryTracker.INSTANCE);
        store.ensureHeavy(readA, storeCursors, EmptyMemoryTracker.INSTANCE);
        store.ensureHeavy(readB, storeCursors, EmptyMemoryTracker.INSTANCE);
        store.ensureHeavy(readC, storeCursors, EmptyMemoryTracker.INSTANCE);
        store.ensureHeavy(readD, storeCursors, EmptyMemoryTracker.INSTANCE);

        assertThat(readA).isEqualTo(tokenA);
        assertThat(readA.isInternal()).isEqualTo(tokenA.isInternal());
        assertThat(readB).isEqualTo(tokenB);
        assertThat(readB.isInternal()).isEqualTo(tokenB.isInternal());
        assertThat(readC).isEqualTo(tokenC);
        assertThat(readC.isInternal()).isEqualTo(tokenC.isInternal());
        assertThat(readD).isEqualTo(tokenD);
        assertThat(readD.isInternal()).isEqualTo(tokenD.isInternal());

        Iterator<NamedToken> itr = store.getAllReadableTokens(storeCursors, EmptyMemoryTracker.INSTANCE)
                .iterator();
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenA", 0));
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenB", 1));
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenC", 2, true));
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenD", 3));

        itr = store.getTokens(storeCursors, EmptyMemoryTracker.INSTANCE).iterator();
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenA", 0));
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenB", 1));
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenC", 2, true));
        assertTrue(itr.hasNext());
        assertThat(itr.next()).isEqualTo(new NamedToken("TokenD", 3));
    }

    private R createInUseRecord(List<DynamicRecord> nameRecords) {
        R tokenRecord = store.newRecord();
        tokenRecord.setId(store.getIdGenerator().nextId(NULL_CONTEXT));
        tokenRecord.initialize(true, nameRecords.get(0).getIntId());
        tokenRecord.addNameRecords(nameRecords);
        tokenRecord.setCreated();
        return tokenRecord;
    }

    private void createEmptyPageZero() throws IOException {
        try (PageCursor cursor = store.pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            // Create an empty page in the file. All records in here will look like they are unused.
            assertTrue(cursor.next());
        }
    }

    private List<DynamicRecord> allocateNameRecords(String tokenName) {
        List<DynamicRecord> nameRecords = new ArrayList<>();
        nameStore.allocateRecordsFromBytes(
                nameRecords,
                tokenName.getBytes(StandardCharsets.UTF_8),
                new StandardDynamicRecordAllocator(
                        store.getNameStore().getIdGenerator(),
                        store.getNameStore().getRecordDataSize()),
                NULL_CONTEXT,
                INSTANCE);
        return nameRecords;
    }

    private void storeToken(R tokenRecord) {
        try (var pageCursor = nameStore.openPageCursorForWriting(0, NULL_CONTEXT);
                var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
            for (DynamicRecord nameRecord : tokenRecord.getNameRecords()) {
                nameStore.updateRecord(nameRecord, pageCursor, NULL_CONTEXT, storeCursors);
            }
            store.updateRecord(tokenRecord, storeCursor, NULL_CONTEXT, storeCursors);
        }
    }
}
