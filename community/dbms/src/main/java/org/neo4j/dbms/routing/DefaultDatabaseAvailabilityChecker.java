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
package org.neo4j.dbms.routing;

import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.kernel.database.DatabaseReferenceImpl;

public final class DefaultDatabaseAvailabilityChecker implements DatabaseAvailabilityChecker {
    private final DatabaseContextProvider<?> databaseContextProvider;

    public DefaultDatabaseAvailabilityChecker(DatabaseContextProvider<?> databaseContextProvider) {
        this.databaseContextProvider = databaseContextProvider;
    }

    @Override
    public boolean isAvailable(DatabaseReferenceImpl.Internal databaseReference) {
        var databaseCtx = databaseContextProvider.getDatabaseContext(databaseReference.databaseId());
        return databaseCtx
                .map(ctx -> ctx.database().getDatabaseAvailabilityGuard().isAvailable())
                .orElse(false);
    }

    @Override
    public boolean isPresent(DatabaseReferenceImpl.Internal databaseReference) {
        return databaseContextProvider
                .getDatabaseContext(databaseReference.databaseId())
                .isPresent();
    }
}
