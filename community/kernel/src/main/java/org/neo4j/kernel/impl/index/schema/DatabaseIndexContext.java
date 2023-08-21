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

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EmptyDependencyResolver;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.monitoring.Monitors;

public class DatabaseIndexContext {
    final PageCache pageCache;
    final CursorContextFactory contextFactory;
    final PageCacheTracer pageCacheTracer;
    final FileSystemAbstraction fileSystem;
    final Monitors monitors;
    final String monitorTag;
    final DatabaseReadOnlyChecker readOnlyChecker;
    final String databaseName;
    final DependencyResolver dependencyResolver;

    private DatabaseIndexContext(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            Monitors monitors,
            String monitorTag,
            DatabaseReadOnlyChecker readOnlyChecker,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            String databaseName,
            DependencyResolver dependencyResolver) {
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
        this.monitors = monitors;
        this.monitorTag = monitorTag;
        this.readOnlyChecker = readOnlyChecker;
        this.contextFactory = contextFactory;
        this.pageCacheTracer = pageCacheTracer;
        this.databaseName = databaseName;
        this.dependencyResolver = dependencyResolver;
    }

    /**
     * @param pageCache global {@link PageCache}
     * @param fileSystem global {@link FileSystemAbstraction}
     * @param databaseName database name
     * @return {@link Builder} to use for creating {@link DatabaseIndexContext}.
     */
    public static Builder builder(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            String databaseName) {
        return new Builder(pageCache, fileSystem, contextFactory, pageCacheTracer, databaseName);
    }

    /**
     * @param copy {@link DatabaseIndexContext} for with to create copy builder from.
     *                                         Note that object references are shared.
     * @return {@link Builder} to use for creating {@link DatabaseIndexContext}, pre-loaded with
     *         all fields from copy.
     */
    public static Builder builder(DatabaseIndexContext copy) {
        return new Builder(
                        copy.pageCache, copy.fileSystem, copy.contextFactory, copy.pageCacheTracer, copy.databaseName)
                .withReadOnlyChecker(copy.readOnlyChecker)
                .withMonitors(copy.monitors)
                .withTag(copy.monitorTag);
    }

    public static class Builder {
        private final PageCache pageCache;
        private final CursorContextFactory contextFactory;
        private final PageCacheTracer pageCacheTracer;
        private final FileSystemAbstraction fileSystem;
        private final String databaseName;
        private Monitors monitors;
        private String monitorTag;
        private DatabaseReadOnlyChecker readOnlyChecker;
        private DependencyResolver dependencyResolver;

        private Builder(
                PageCache pageCache,
                FileSystemAbstraction fileSystem,
                CursorContextFactory contextFactory,
                PageCacheTracer pageCacheTracer,
                String databaseName) {
            this.pageCache = pageCache;
            this.fileSystem = fileSystem;
            this.contextFactory = contextFactory;
            this.pageCacheTracer = pageCacheTracer;
            this.databaseName = databaseName;
            this.monitors = new Monitors();
            this.monitorTag = "";
            this.readOnlyChecker = DatabaseReadOnlyChecker.writable();
            this.dependencyResolver = EmptyDependencyResolver.EMPTY_RESOLVER;
        }

        /**
         * Default is false
         *
         * @param readOnlyChecker checks if underlying database is readonly.
         * @return {@link Builder this builder}
         */
        public Builder withReadOnlyChecker(DatabaseReadOnlyChecker readOnlyChecker) {
            this.readOnlyChecker = readOnlyChecker;
            return this;
        }

        /**
         * Default is new empty {@link Monitors}.
         *
         * @param monitors {@link Monitors monitors} to use.
         * @return {@link Builder this builder}
         */
        public Builder withMonitors(Monitors monitors) {
            this.monitors = monitors;
            return this;
        }

        /**
         * Default is empty string.
         *
         * @param monitorTag {@link String} to use as tag for monitor listeners.
         * @return {@link Builder this builder}
         */
        public Builder withTag(String monitorTag) {
            this.monitorTag = monitorTag;
            return this;
        }

        /**
         * Default is empty resolver.
         * @param dependencyResolver database {@link DependencyResolver} that can be used by index to look up components.
         * @return {@link Builder this builder}
         */
        public Builder withDependencyResolver(DependencyResolver dependencyResolver) {
            this.dependencyResolver = dependencyResolver;
            return this;
        }

        public DatabaseIndexContext build() {
            return new DatabaseIndexContext(
                    pageCache,
                    fileSystem,
                    monitors,
                    monitorTag,
                    readOnlyChecker,
                    contextFactory,
                    pageCacheTracer,
                    databaseName,
                    dependencyResolver);
        }
    }
}
