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

import java.io.IOException;
import org.neo4j.counts.CountsStorage;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * Store for degrees of relationship chains for dense nodes. Relationship group record ID plus relationship direction forms the key for the counts.
 */
public interface RelationshipGroupDegreesStore extends CountsStorage<DegreeUpdater> {
    /**
     * @param groupId the relationship group ID to look for.
     * @param direction the direction to look for.
     * @param cursorContext page cache access context.
     * @return the degree for the given groupId and direction, or {@code 0} if it wasn't found.
     */
    long degree(long groupId, RelationshipDirection direction, CursorContext cursorContext);

    /**
     * Accepts a visitor observing all entries in this store.
     * @param visitor to receive the entries.
     * @param cursorContext page cache access context.
     */
    void accept(GroupDegreeVisitor visitor, CursorContext cursorContext);

    default DegreeUpdater directApply(CursorContext cursorContext) throws IOException {
        return apply(TransactionIdStore.BASE_TX_ID, true, cursorContext);
    }

    interface GroupDegreeVisitor {
        /**
         * Receives data about a degree.
         * @param groupId relationship group ID of the degree.
         * @param direction direction of the degree.
         * @param degree the absolute degree for the group and direction.
         */
        void degree(long groupId, RelationshipDirection direction, long degree);
    }
}
