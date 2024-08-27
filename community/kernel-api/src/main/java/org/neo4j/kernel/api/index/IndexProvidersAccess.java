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
package org.neo4j.kernel.api.index;

import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.TokenHolders;

public interface IndexProvidersAccess {
    IndexProviderMap access(
            PageCache pageCache,
            DatabaseLayout layout,
            DatabaseReadOnlyChecker readOnlyChecker,
            MemoryTracker memoryTracker);

    IndexProviderMap access(
            PageCache pageCache,
            DatabaseLayout layout,
            DatabaseReadOnlyChecker readOnlyChecker,
            TokenHolders tokenHolders);
}
