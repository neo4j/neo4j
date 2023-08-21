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

import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseReferenceImpl;

/**
 * Instances implementing this interface allow routing validators to check whether it is valid to serve a routing
 * table for a given database, when requested.
 *
 * A database may be invalid for routing because it is not currently available (i.e. it may be store copying) or not
 * present at all (perhaps it is still starting up, or dropped, or only exists on another Neo4j instance).
 *
 * @see LocalRoutingTableServiceValidator
 */
public interface DatabaseAvailabilityChecker {
    /**
     * Note: availability here is in the sense of {@link DatabaseAvailabilityGuard}
     *
     * @param databaseReference the database reference to check
     * @return whether this database is currently available
     */
    boolean isAvailable(DatabaseReferenceImpl.Internal databaseReference);

    /**
     * @param databaseReference the database reference to check
     * @return whether this database is present on this Neo4j instance
     */
    boolean isPresent(DatabaseReferenceImpl.Internal databaseReference);
}
