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
package org.neo4j.dbms.identity;

import java.util.Optional;
import java.util.UUID;
import org.neo4j.util.Id;

/**
 * ServerId is used for identifying a Neo4J instance.
 * It is persisted in the root of the data directory.
 */
public final class ServerId extends Id implements Comparable<ServerId> {

    private static final int UUID_LEN = 36;

    /**
     * Parse a full UUID based ServerId
     * @param uuid a legit UUID
     * @return a ServerId if the uuid is valid OR {@link Optional#empty()}
     */
    public static Optional<ServerId> from(String uuid) {
        if (uuid != null && uuid.length() == UUID_LEN) {
            try {
                return Optional.of(new ServerId(UUID.fromString(uuid)));
            } catch (IllegalArgumentException ignore) {

            }
        }
        return Optional.empty();
    }

    public ServerId(UUID uuid) {
        super(uuid);
    }

    @Override
    public String toString() {
        return "ServerId{" + shortName() + '}';
    }

    @Override
    public int compareTo(ServerId other) {
        return this.uuid().compareTo(other.uuid());
    }
}
