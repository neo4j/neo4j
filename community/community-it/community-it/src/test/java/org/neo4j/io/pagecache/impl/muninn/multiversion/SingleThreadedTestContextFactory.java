/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.impl.muninn.multiversion;

import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.util.function.LongSupplier;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.context.TransactionVersionContext;

class SingleThreadedTestContextFactory extends CursorContextFactory {
    private final VersionContextSupplier versionContextSupplier;
    private final PageCacheTracer cacheTracer;

    SingleThreadedTestContextFactory(PageCacheTracer pageCacheTracer) {
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

    static class TestCursorContext extends CursorContext {
        private final TestTransactionVersionContext versionContext;

        protected TestCursorContext(
                CursorContextFactory contextFactory, PageCursorTracer cursorTracer, VersionContext versionContext) {
            super(contextFactory, cursorTracer, versionContext);
            this.versionContext = (TestTransactionVersionContext) versionContext;
        }

        public void setWriteAndReadVersion(long version) {
            setWriteAndReadVersion(version, version);
        }

        public void setWriteAndReadVersion(long writeVersion, long readVersion) {
            versionContext.closedTxIdSupplier.setLastClosedTxId(readVersion);
            versionContext.initRead();
            versionContext.initWrite(writeVersion);
        }
    }

    private static class TestVersionContextSupplier implements VersionContextSupplier {
        private final MutableLongClosedTxIdSupplier closedTxIdSupplier = new MutableLongClosedTxIdSupplier();

        @Override
        public void init(LongSupplier lastClosedTransactionIdSupplier) {}

        @Override
        public VersionContext createVersionContext() {
            return new TestTransactionVersionContext(closedTxIdSupplier);
        }
    }

    private static class TestTransactionVersionContext extends TransactionVersionContext {

        private final MutableLongClosedTxIdSupplier closedTxIdSupplier;

        TestTransactionVersionContext(MutableLongClosedTxIdSupplier closedTxIdSupplier) {
            super(closedTxIdSupplier);
            this.closedTxIdSupplier = closedTxIdSupplier;
        }
    }

    private static class MutableLongClosedTxIdSupplier implements LongSupplier {
        private final MutableLong lastClosedTxId = new MutableLong();

        public void setLastClosedTxId(long value) {
            lastClosedTxId.setValue(value);
        }

        @Override
        public long getAsLong() {
            return lastClosedTxId.toLong();
        }
    }
}
