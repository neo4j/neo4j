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
package org.neo4j.index.internal.gbptree;

import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Internal log for capturing changes to the structural changes happening to a GBPTree as part of writers
 * making changes. Structural changes include creating node tree nodes (from splits), deleting tree nodes
 * (from merge) and creating successors.
 */
public interface StructureWriteLog extends AutoCloseable {
    Empty EMPTY = new Empty();

    Session newSession();

    void checkpoint(long previousStableGeneration, long newStableGeneration, long newUnstableGeneration);

    void close();

    static StructureWriteLog structureWriteLog(FileSystemAbstraction fs, Path gbpTreeFile, Config config) {
        if (config.get(GraphDatabaseInternalSettings.gbptree_structure_log_enabled)) {
            return LoggingStructureWriteLog.forGBPTree(fs, gbpTreeFile);
        }
        return EMPTY;
    }

    interface Session {
        void split(long generation, long parentId, long childId, long createdChildId);

        void merge(long generation, long parentId, long childId, long deletedChildId);

        void createSuccessor(long generation, long parentId, long oldId, long newId);

        void addToFreelist(long generation, long id);

        void growTree(long generation, long createdRootId);

        void shrinkTree(long generation, long deletedRootId);
    }

    class Empty implements StructureWriteLog, Session {
        @Override
        public Session newSession() {
            return this;
        }

        @Override
        public void split(long generation, long parentId, long childId, long createdChildId) {}

        @Override
        public void merge(long generation, long parentId, long childId, long deletedChildId) {}

        @Override
        public void createSuccessor(long generation, long parentId, long oldId, long newId) {}

        @Override
        public void addToFreelist(long generation, long id) {}

        @Override
        public void growTree(long generation, long createdRootId) {}

        @Override
        public void shrinkTree(long generation, long deletedRootId) {}

        @Override
        public void checkpoint(long previousStableGeneration, long newStableGeneration, long newUnstableGeneration) {}

        @Override
        public void close() {}
    }
}
