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
package org.neo4j.storageengine.util;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;

public interface IdUpdateListener extends AutoCloseable {
    void markIdAsUsed(IdGenerator idGenerator, long id, int size, CursorContext cursorContext);

    void markIdAsUnused(IdGenerator idGenerator, long id, int size, CursorContext cursorContext);

    default void markId(IdGenerator idGenerator, long id, int size, boolean used, CursorContext cursorContext) {
        if (used) {
            markIdAsUsed(idGenerator, id, size, cursorContext);
        } else {
            markIdAsUnused(idGenerator, id, size, cursorContext);
        }
    }

    IdUpdateListener DIRECT = new IdUpdateListener() {
        @Override
        public void close() {
            // no-op
        }

        @Override
        public void markIdAsUsed(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            try (var marker = idGenerator.transactionalMarker(cursorContext)) {
                marker.markUsed(id, size);
            }
        }

        @Override
        public void markIdAsUnused(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            try (var marker = idGenerator.transactionalMarker(cursorContext)) {
                marker.markDeleted(id, size);
            }
        }
    };

    IdUpdateListener DIRECT_WITH_FREE_ON_DELETE = new IdUpdateListener() {
        @Override
        public void close() {
            // no-op
        }

        @Override
        public void markIdAsUsed(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            try (var marker = idGenerator.transactionalMarker(cursorContext)) {
                marker.markUsed(id, size);
            }
        }

        @Override
        public void markIdAsUnused(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            try (var marker = idGenerator.transactionalMarker(cursorContext)) {
                marker.markDeletedAndFree(id, size);
            }
        }
    };

    IdUpdateListener IGNORE = new IdUpdateListener() {
        @Override
        public void close() {
            // no-op
        }

        @Override
        public void markIdAsUsed(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) { // no-op
        }

        @Override
        public void markIdAsUnused(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) { // no-op
        }
    };
}
