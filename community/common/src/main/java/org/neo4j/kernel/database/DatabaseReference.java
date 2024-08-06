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

import java.util.Optional;
import java.util.UUID;

/**
 * Implementations of this interface represent different kinds of Database reference.
 * A database may have multiple references, each with a different alias.
 * The reference whose {@link #alias()} corresponds to the database's original name is known as the primary reference.
 */
public interface DatabaseReference extends Comparable<DatabaseReference>, PrivilegeDatabaseReference {
    NormalizedDatabaseName alias();

    /**
     * @return the namespace that the alias is in, or empty if it is in the default namespace
     */
    Optional<NormalizedDatabaseName> namespace();

    /**
     * @return whether the alias associated with this reference is the database's original/true name
     */
    boolean isPrimary();

    /**
     * @return the unique identity for this reference
     */
    UUID id();

    /**
     * @return Prettified String representaion
     */
    String toPrettyString();

    /**
     * @return the full normalized name of the dataspace, including the namespace.
     */
    NormalizedDatabaseName fullName();

    @Override
    default String name() {
        return fullName().name();
    }

    /**
     * @return the owning database of this reference. This is used for authorization on property shards which inherit
     * their permissions from the "owning" graph shard.
     */
    String owningDatabaseName();

    /**
     * @return true if this reference points to a Composite database, otherwise false
     */
    boolean isComposite();
}
