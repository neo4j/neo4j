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
package org.neo4j.kernel.database;

import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

public final class DatabaseIdHelper {
    private DatabaseIdHelper() { // no-op
    }

    public static NamedDatabaseId randomNamedDatabaseId() {
        return new NamedDatabaseId(RandomStringUtils.randomAlphabetic(20), UUID.randomUUID());
    }

    public static DatabaseId randomDatabaseId() {
        return new DatabaseId(UUID.randomUUID());
    }
}
