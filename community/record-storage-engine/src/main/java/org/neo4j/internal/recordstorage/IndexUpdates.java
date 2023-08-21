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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.IndexEntryUpdate;

/**
 * Set of updates ({@link IndexEntryUpdate}) to apply to indexes.
 */
public interface IndexUpdates extends Iterable<IndexEntryUpdate<IndexDescriptor>>, AutoCloseable {
    /**
     * Feeds updates raw material in the form of node/property commands, to create updates from.
     *
     * @param nodeCommands         node data
     * @param relationshipCommands relationship data
     * @param commandSelector
     */
    void feed(
            EntityCommandGrouper<NodeCommand>.Cursor nodeCommands,
            EntityCommandGrouper<RelationshipCommand>.Cursor relationshipCommands,
            CommandSelector commandSelector);

    boolean hasUpdates();

    @Override
    void close();

    /**
     * Cleans collected updates. Instance should be ready for re-use after this.
     */
    void reset();
}
