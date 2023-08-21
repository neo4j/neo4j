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
package org.neo4j.storageengine.migration;

import java.io.IOException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.storageengine.api.StoreVersion;

/**
 * Default empty implementation of StoreMigrationParticipant.
 * Base class for all StoreMigrationParticipant implementations.
 */
public abstract class AbstractStoreMigrationParticipant implements StoreMigrationParticipant {
    protected final String name;

    protected AbstractStoreMigrationParticipant(String name) {
        this.name = name;
    }

    @Override
    public void postMigration(
            DatabaseLayout databaseLayout, StoreVersion toVersion, long txIdBeforeMigration, long txIdAfterMigration)
            throws IOException {
        // no-op
    }

    @Override
    public String getName() {
        return name;
    }
}
