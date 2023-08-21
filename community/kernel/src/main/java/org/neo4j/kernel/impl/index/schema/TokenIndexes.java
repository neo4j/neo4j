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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.kernel.impl.index.schema.TokenIndex.FAILED;
import static org.neo4j.kernel.impl.index.schema.TokenIndex.ONLINE;
import static org.neo4j.kernel.impl.index.schema.TokenIndex.POPULATING;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;

public final class TokenIndexes {
    private TokenIndexes() {}

    public static InternalIndexState readState(
            PageCache pageCache,
            Path indexFile,
            String databaseName,
            CursorContext cursorContext,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        NativeIndexHeaderReader headerReader = new NativeIndexHeaderReader(FAILED);
        GBPTree.readHeader(pageCache, indexFile, headerReader, databaseName, cursorContext, openOptions);
        return switch (headerReader.state) {
            case FAILED -> InternalIndexState.FAILED;
            case ONLINE -> InternalIndexState.ONLINE;
            case POPULATING -> InternalIndexState.POPULATING;
            default -> throw new IllegalStateException("Unexpected initial state byte value " + headerReader.state);
        };
    }

    static String readFailureMessage(
            PageCache pageCache,
            Path indexFile,
            String databaseName,
            CursorContext cursorContext,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        NativeIndexHeaderReader headerReader = new NativeIndexHeaderReader(FAILED);
        GBPTree.readHeader(pageCache, indexFile, headerReader, databaseName, cursorContext, openOptions);
        return headerReader.failureMessage;
    }
}
