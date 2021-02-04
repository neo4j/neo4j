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
package org.neo4j.internal.counts;

import org.neo4j.counts.CountsStorage;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipDirection;

/**
 * Store for degrees of relationship chains for dense nodes. Relationship group record ID plus relationship direction forms the key for the counts.
 */
public interface RelationshipGroupDegreesStore extends CountsStorage
{
    /**
     * @param txId for which transaction ID the changes will be made.
     * @param cursorTracer tracer for page cache access.
     * @return an {@link Updater} which is able to make counts updates.
     */
    Updater apply( long txId, PageCursorTracer cursorTracer );

    /**
     * @param groupId the relationship group ID to look for.
     * @param direction the direction to look for.
     * @param cursorTracer tracer for page cache access.
     * @return the degree for the given groupId and direction, or {@code 0} if it wasn't found.
     */
    long degree( long groupId, RelationshipDirection direction, PageCursorTracer cursorTracer );

    /**
     * Accepts a visitor observing all entries in this store.
     * @param visitor to receive the entries.
     * @param cursorTracer tracer for page cache access.
     */
    void accept( GroupDegreeVisitor visitor, PageCursorTracer cursorTracer );

    interface Updater extends AutoCloseable
    {
        @Override
        void close();

        /**
         * Changes the degree of the given groupId and direction.
         *
         * @param groupId the relationship group ID to make the change for.
         * @param direction the direction to make the change for.
         * @param delta delta value to apply, can be either positive or negative.
         */
        void increment( long groupId, RelationshipDirection direction, long delta );
    }

    interface GroupDegreeVisitor
    {
        /**
         * Receives data about a degree.
         * @param groupId relationship group ID of the degree.
         * @param direction direction of the degree.
         * @param degree the absolute degree for the group and direction.
         */
        void degree( long groupId, RelationshipDirection direction, long degree );
    }
}
