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
package org.neo4j.io.pagecache.context;

import static java.util.Objects.requireNonNull;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.context.FixedVersionContext.EMPTY_VERSION_CONTEXT;

import org.neo4j.io.pagecache.tracing.cursor.CursorStatisticSnapshot;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public class CursorContext implements AutoCloseable {
    public static final CursorContext NULL_CONTEXT = new NullCursorContext();
    public static final CursorContext INITIALIZATION_SENTINEL_CONTEXT = new InitializationCursorContext();

    private final PageCursorTracer cursorTracer;
    private final VersionContext versionContext;
    private final CursorContext noCurrentTransactionContext;
    private final CursorContextFactory contextFactory;
    private final boolean includeCurrentTransaction;

    protected CursorContext(
            CursorContextFactory contextFactory, PageCursorTracer cursorTracer, VersionContext versionContext) {
        this(
                contextFactory,
                requireNonNull(cursorTracer),
                requireNonNull(versionContext),
                new CursorContext(contextFactory, cursorTracer, versionContext, null, false),
                true);
    }

    private CursorContext(
            CursorContextFactory contextFactory,
            PageCursorTracer cursorTracer,
            VersionContext versionContext,
            CursorContext noCurrentTransactionContext,
            boolean includeCurrentTransaction) {
        this.contextFactory = contextFactory;
        this.cursorTracer = requireNonNull(cursorTracer);
        this.versionContext = requireNonNull(versionContext);
        this.noCurrentTransactionContext = noCurrentTransactionContext == null ? this : noCurrentTransactionContext;
        this.includeCurrentTransaction = includeCurrentTransaction;
    }

    public PageCursorTracer getCursorTracer() {
        return cursorTracer;
    }

    public VersionContext getVersionContext() {
        return versionContext;
    }

    public CursorContext noCurrentTransactionContext() {
        return noCurrentTransactionContext;
    }

    /**
     * @return if cursors using this context should include versions created by the current transaction
     */
    public boolean includeCurrentTransaction() {
        return includeCurrentTransaction;
    }

    @Override
    public void close() {
        cursorTracer.close();
    }

    public void merge(CursorStatisticSnapshot statisticSnapshot) {
        cursorTracer.merge(statisticSnapshot);
    }

    public CursorContext createRelatedContext(String tag) {
        return contextFactory.create(tag, versionContext.createRelatedContext());
    }

    /**
     * Create CursorContext with VersionContext that provides unlimited visibility and also can be used to commit different versions
     */
    public CursorContext createUnboundedReadRelatedContext(String tag) {
        return contextFactory.create(tag, versionContext.createUnboundedReadRelatedContext());
    }

    public CursorContext createUnboundedRelatedContext() {
        return new CursorContext(contextFactory, cursorTracer, EMPTY_VERSION_CONTEXT);
    }

    private static class NullCursorContext extends CursorContext {
        private NullCursorContext() {
            super(NULL_CONTEXT_FACTORY, PageCursorTracer.NULL, EMPTY_VERSION_CONTEXT);
        }

        @Override
        public String toString() {
            return "!!!THIS IS THE NULL CURSOR CONTEXT!!!";
        }
    }

    private static class InitializationCursorContext extends CursorContext {
        private InitializationCursorContext() {
            super(NULL_CONTEXT_FACTORY, PageCursorTracer.NULL, EMPTY_VERSION_CONTEXT);
        }

        @Override
        public String toString() {
            return "CONTEXT IS INITIALIZING... PLEASE COME BACK SOON";
        }
    }
}
