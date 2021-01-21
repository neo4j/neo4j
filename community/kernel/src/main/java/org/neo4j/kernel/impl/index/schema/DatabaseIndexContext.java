/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.monitoring.Monitors;

public class DatabaseIndexContext
{
    final PageCache pageCache;
    final FileSystemAbstraction fileSystem;
    final Monitors monitors;
    final String monitorTag;
    final boolean readOnly;

    private DatabaseIndexContext( PageCache pageCache, FileSystemAbstraction fileSystem, Monitors monitors, String monitorTag, boolean readOnly )
    {
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
        this.monitors = monitors;
        this.monitorTag = monitorTag;
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

    /**
     * @param copy {@link DatabaseIndexContext} for with to create copy builder from.
     *                                         Note that object references are shared.
     * @return {@link Builder} to use for creating {@link DatabaseIndexContext}, pre-loaded with
     *         all fields from copy.
     */
    public static Builder builder( DatabaseIndexContext copy )
    {
        return new Builder( copy.pageCache, copy.fileSystem )
                .withReadOnly( copy.readOnly )
                .withMonitors( copy.monitors )
                .withTag( copy.monitorTag );
    }

    public static class Builder
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fileSystem;
        private Monitors monitors;
        private String monitorTag;
        private boolean readOnly;

        private Builder( PageCache pageCache, FileSystemAbstraction fileSystem )
        {
            this.pageCache = pageCache;
            this.fileSystem = fileSystem;
            this.monitors = new Monitors();
            this.monitorTag = "";
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
         * Default is new empty {@link Monitors}.
         *
         * @param monitors {@link Monitors monitors} to use.
         * @return {@link Builder this builder}
         */
        public Builder withMonitors( Monitors monitors )
        {
            this.monitors = monitors;
            return this;
        }

        /**
         * Default is empty string.
         *
         * @param monitorTag {@link String} to use as tag for monitor listeners.
         * @return {@link Builder this builder}
         */
        public Builder withTag( String monitorTag )
        {
            this.monitorTag = monitorTag;
            return this;
        }

        public DatabaseIndexContext build()
        {
            return new DatabaseIndexContext( pageCache, fileSystem, monitors, monitorTag, readOnly );
        }
    }
}
