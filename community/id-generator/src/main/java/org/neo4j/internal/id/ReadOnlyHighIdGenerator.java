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
package org.neo4j.internal.id;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * An {@link IdGenerator} that knows only about a highId and is read-only
 */
class ReadOnlyHighIdGenerator implements IdGenerator {
    private final long highId;
    private final IdType idType;

    ReadOnlyHighIdGenerator(long highId, IdType idType) {
        this.highId = highId;
        this.idType = idType;
    }

    @Override
    public void setHighId(long id) {
        throw new UnsupportedOperationException("Should not be required");
    }

    @Override
    public void markHighestWrittenAtHighId() {
        throw new UnsupportedOperationException("Should not be required");
    }

    @Override
    public long getHighestWritten() {
        return highId;
    }

    @Override
    public long getHighId() {
        return highId;
    }

    @Override
    public long getHighestPossibleIdInUse() {
        return highId - 1;
    }

    @Override
    public Marker marker(CursorContext cursorContext) {
        throw new UnsupportedOperationException("Should not be required");
    }

    @Override
    public void close() {
        // It's fine, there's nothing to close
    }

    @Override
    public long getNumberOfIdsInUse() {
        return getHighId();
    }

    @Override
    public long getDefragCount() {
        // Doesn't quite matter actually, not for the intended use case anyway
        return 0;
    }

    @Override
    public void checkpoint(CursorContext cursorContext) {
        // no-op
    }

    @Override
    public void maintenance(CursorContext cursorContext) {
        throw new UnsupportedOperationException("Should not be required");
    }

    @Override
    public void start(FreeIds freeIdsForRebuild, CursorContext cursorContext) {
        // no-op
    }

    @Override
    public long nextId(CursorContext ignored) {
        throw new UnsupportedOperationException("Should not be required");
    }

    @Override
    public long nextConsecutiveIdRange(int numberOfIds, boolean favorSamePage, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Should not be required");
    }

    @Override
    public void clearCache(CursorContext cursorContext) {
        // no-op
    }

    @Override
    public IdType idType() {
        return idType;
    }

    @Override
    public boolean consistencyCheck(ReporterFactory reporterFactory, CursorContext cursorContext) {
        return true;
    }
}
