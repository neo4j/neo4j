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
package org.neo4j.router.impl;

import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.router.location.LocationService;

public class CommunityLocationService implements LocationService {

    @Override
    public Location locationOf(DatabaseReference databaseReference) {
        if (databaseReference instanceof DatabaseReferenceImpl.Internal ref) {
            return new Location.Local(-1, ref);
        }
        throw new IllegalArgumentException("Unexpected DatabaseReference type: " + databaseReference);
    }
}
