/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;

public class DatabaseIndexContext
{
    final PageCache pageCache;
    final FileSystemAbstraction fileSystem;
    final IndexProvider.Monitor monitor;
    final boolean readOnly;

    private DatabaseIndexContext( PageCache pageCache, FileSystemAbstraction fileSystem, IndexProvider.Monitor monitor, boolean readOnly )
    {
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
        this.monitor = monitor;
        this.readOnly = readOnly;
    }

    /**
     * @param pageCache global {@link PageCache}
     * @param fileSystem global {@link FileSystemAbstraction}
     * @return {@link Builder} to use for creating {@link DatabaseIndexContext}.
     */
    public static Builder builder( PageCache pageCache, FileSystemAbstraction fileSystem )
    {
        return new Builder( pageCache, fileSystem );
    }

    public static class Builder
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fileSystem;
        private IndexProvider.Monitor monitor;
        private boolean readOnly;

        private Builder( PageCache pageCache, FileSystemAbstraction fileSystem )
        {
            this.pageCache = pageCache;
            this.fileSystem = fileSystem;
            this.monitor = IndexProvider.Monitor.EMPTY;
            this.readOnly = false;
        }

        /**
         * Default is false
         *
         * @param readOnly true no writes allowed.
         * @return {@link Builder this builder}
         */
        public Builder withReadOnly( boolean readOnly )
        {
            this.readOnly = readOnly;
            return this;
        }

        /**
         * Default is {@link IndexProvider.Monitor#EMPTY}.
         *
         * @param monitor {@link IndexProvider.Monitor monitor} to use.
         * @return {@link Builder this builder}
         */
        public Builder withMonitor( IndexProvider.Monitor monitor )
        {
            this.monitor = monitor;
            return this;
        }

        public DatabaseIndexContext build()
        {
            return new DatabaseIndexContext( pageCache, fileSystem, monitor, readOnly );
        }
    }
}
