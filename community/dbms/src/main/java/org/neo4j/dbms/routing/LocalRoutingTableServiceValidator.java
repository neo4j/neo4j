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

import org.neo4j.kernel.database.DatabaseReferenceImpl;

public class LocalRoutingTableServiceValidator implements RoutingTableServiceValidator {

    private final DatabaseAvailabilityChecker databaseAvailabilityChecker;

    public LocalRoutingTableServiceValidator(DatabaseAvailabilityChecker databaseAvailabilityChecker) {
        this.databaseAvailabilityChecker = databaseAvailabilityChecker;
    }

    @Override
    public void isValidForServerSideRouting(DatabaseReferenceImpl.Internal databaseReference) throws RoutingException {
        assertDatabaseIsOperational(databaseReference);
    }

    @Override
    public void isValidForClientSideRouting(DatabaseReferenceImpl.Internal databaseReference) throws RoutingException {
        assertDatabaseIsOperational(databaseReference);
    }

    private void assertDatabaseIsOperational(DatabaseReferenceImpl.Internal databaseReference) throws RoutingException {
        if (!databaseAvailabilityChecker.isPresent(databaseReference)) {
            throw RoutingTableServiceHelpers.databaseNotFoundException(
                    databaseReference.alias().name());
        }
        if (!databaseAvailabilityChecker.isAvailable(databaseReference)) {
            throw RoutingTableServiceHelpers.databaseNotAvailableException(
                    databaseReference.alias().name());
        }
    }
}
