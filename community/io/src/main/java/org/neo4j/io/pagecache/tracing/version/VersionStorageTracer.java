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
package org.neo4j.io.pagecache.tracing.version;

import org.neo4j.io.pagecache.tracing.FileFlushEvent;

public interface VersionStorageTracer {
    VersionStorageTracer NULL = new VersionStorageTracer() {
        @Override
        public RegionCollectionEvent beginRegionCollection() {
            return RegionCollectionEvent.NULL;
        }

        @Override
        public RegionSwitchEvent regionSwitch() {
            return RegionSwitchEvent.NULL;
        }

        @Override
        public FileFlushEvent beginFileFlush() {
            return FileFlushEvent.NULL;
        }

        @Override
        public void allowPageFlush(long pageRef, boolean allowFlush) {}
    };

    RegionCollectionEvent beginRegionCollection();

    RegionSwitchEvent regionSwitch();

    FileFlushEvent beginFileFlush();

    void allowPageFlush(long pageRef, boolean allowFlush);
}
