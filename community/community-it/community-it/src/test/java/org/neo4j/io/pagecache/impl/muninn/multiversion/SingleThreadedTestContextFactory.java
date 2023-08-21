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
package org.neo4j.io.pagecache.impl.muninn.multiversion;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.OldestTransactionIdFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.context.TransactionVersionContext;

public class SingleThreadedTestContextFactory extends CursorContextFactory {
    private final VersionContextSupplier versionContextSupplier;
    private final PageCacheTracer cacheTracer;

    public SingleThreadedTestContextFactory(PageCacheTracer pageCacheTracer) {
        this(pageCacheTracer, new TestVersionContextSupplier());
    }

    SingleThreadedTestContextFactory(PageCacheTracer cacheTracer, VersionContextSupplier versionContextSupplier) {
        super(cacheTracer, versionContextSupplier);
        this.versionContextSupplier = versionContextSupplier;
        this.cacheTracer = cacheTracer;
    }

    @Override
    public TestCursorContext create(String tag) {
        TestCursorContext testCursorContext = new TestCursorContext(
                this, cacheTracer.createPageCursorTracer(tag), versionContextSupplier.createVersionContext());
        testCursorContext.setWriteAndReadVersion(BASE_TX_ID);
        return testCursorContext;
    }

    public static class TestCursorContext extends CursorContext {
        private final TestTransactionVersionContext versionContext;

        protected TestCursorContext(
                CursorContextFactory contextFactory, PageCursorTracer cursorTracer, VersionContext versionContext) {
            super(contextFactory, cursorTracer, versionContext);
            this.versionContext = (TestTransactionVersionContext) versionContext;
        }

        public void setWriteAndReadVersion(long version, long[] nonVisibleIds) {
            setWriteAndReadVersion(version, version, nonVisibleIds);
        }

        public void setWriteAndReadVersion(long writeVersion, long readVersion) {
            setWriteAndReadVersion(writeVersion, readVersion, EMPTY_LONG_ARRAY);
        }

        public void setWriteAndReadVersion(long version) {
            setWriteAndReadVersion(version, version, EMPTY_LONG_ARRAY);
        }

        public void setWriteAndReadVersion(long writeVersion, long readVersion, long[] notVisibleIds) {
            MutableTransactionSnapshotSupplier snapshotSupplier = versionContext.snapshotSupplier;
            snapshotSupplier.setLastClosedTxId(readVersion);
            snapshotSupplier.setHigherBoundaries(readVersion, notVisibleIds);
            versionContext.initRead();
            versionContext.initWrite(writeVersion);
        }
    }

    private static class TestVersionContextSupplier implements VersionContextSupplier {
        private final MutableTransactionSnapshotSupplier snapshotSupplier = new MutableTransactionSnapshotSupplier();

        @Override
        public void init(
                TransactionIdSnapshotFactory transactionIdSnapshotFactory,
                OldestTransactionIdFactory oldestTransactionIdFactory) {}

        @Override
        public VersionContext createVersionContext() {
            return new TestTransactionVersionContext(snapshotSupplier);
        }
    }

    private static class TestTransactionVersionContext extends TransactionVersionContext {

        private final MutableTransactionSnapshotSupplier snapshotSupplier;

        TestTransactionVersionContext(MutableTransactionSnapshotSupplier snapshotSupplier) {
            super(snapshotSupplier, OldestTransactionIdFactory.EMPTY_OLDEST_ID_FACTORY);
            this.snapshotSupplier = snapshotSupplier;
        }
    }

    private static class MutableTransactionSnapshotSupplier implements TransactionIdSnapshotFactory {
        private long lastClosedTxId;
        private long highestVisible;
        private long[] nonVisibleIds = EMPTY_LONG_ARRAY;

        public void setLastClosedTxId(long value) {
            lastClosedTxId = value;
            highestVisible = value;
        }

        public void setHigherBoundaries(long highestVisible, long[] notVisibleIds) {
            this.highestVisible = highestVisible;
            this.nonVisibleIds = notVisibleIds;
        }

        @Override
        public TransactionIdSnapshot createSnapshot() {
            return new TransactionIdSnapshot(lastClosedTxId, highestVisible, nonVisibleIds);
        }
    }
}
