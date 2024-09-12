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
package org.neo4j.batchimport.api;

import java.io.IOException;
import java.util.List;
import org.neo4j.internal.schema.IndexDescriptor;

/**
 * Used by the {@link BatchImporter} to create indexes after it has imported node & relationship entities.
 */
public interface IndexesCreator {

    /**
     * Creates all the provided indexes
     * @param creationListener callback mechanism used to track the status of the creation process
     * @param indexDescriptors the indexes to create
     * @throws IOException if unable to create any indexes
     */
    void create(CreationListener creationListener, List<IndexDescriptor> indexDescriptors) throws IOException;

    interface CreationListener {
        /**
         * Updates interested parties with the latest status of the index creation for the provided {@link IndexDescriptor}
         * @param indexDescriptor the index that has been updated
         * @param percentDelta indicator of how much of the index creation changed from the last update
         */
        void onUpdate(IndexDescriptor indexDescriptor, float percentDelta);

        /**
         * When the index creation for all the indexes have been completed
         */
        void onCreationCompleted();

        /**
         * When the index creation for all the indexes have been check pointed
         */
        void onCheckpointingCompleted();
    }

    IndexesCreator EMPTY_CREATOR = (creationListener, indexDescriptors) -> {};
}
