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
package org.neo4j.dbms;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.neo4j.kernel.database.NamedDatabaseId;

public class StubDatabaseStateService implements DatabaseStateService {
    private final Map<NamedDatabaseId, DatabaseState> databaseStates;
    private final Function<NamedDatabaseId, DatabaseState> unknownFactory;

    public StubDatabaseStateService(Function<NamedDatabaseId, DatabaseState> unknownFactory) {
        this.unknownFactory = unknownFactory;
        this.databaseStates = Collections.emptyMap();
    }

    public StubDatabaseStateService(
            Map<NamedDatabaseId, DatabaseState> databaseStates,
            Function<NamedDatabaseId, DatabaseState> unknownFactory) {
        this.databaseStates = databaseStates;
        this.unknownFactory = unknownFactory;
    }

    @Override
    public DatabaseState stateOfDatabase(NamedDatabaseId namedDatabaseId) {
        var state = databaseStates.get(namedDatabaseId);
        return state == null ? unknownFactory.apply(namedDatabaseId) : state;
    }

    @Override
    public Optional<Throwable> causeOfFailure(NamedDatabaseId namedDatabaseId) {
        return Optional.ofNullable(databaseStates.get(namedDatabaseId)).flatMap(DatabaseState::failure);
    }

    @Override
    public Map<NamedDatabaseId, DatabaseState> stateOfAllDatabases() {
        return Map.copyOf(databaseStates);
    }
}
