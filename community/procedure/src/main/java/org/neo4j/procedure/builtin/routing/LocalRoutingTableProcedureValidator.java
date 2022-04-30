/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.builtin.routing;

import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;

import java.util.Optional;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;

public class LocalRoutingTableProcedureValidator extends BaseRoutingTableProcedureValidator {
    public LocalRoutingTableProcedureValidator(DatabaseContextProvider<?> databaseContextProvider) {
        super(databaseContextProvider);
    }

    @Override
    public void isValidForServerSideRouting(NamedDatabaseId namedDatabaseId) throws ProcedureException {
        assertDatabaseIsOperational(namedDatabaseId);
    }

    @Override
    public void isValidForClientSideRouting(NamedDatabaseId namedDatabaseId) throws ProcedureException {
        assertDatabaseIsOperational(namedDatabaseId);
    }

    private void assertDatabaseIsOperational(NamedDatabaseId namedDatabaseId) throws ProcedureException {
        Optional<Database> database = getDatabase(namedDatabaseId);
        if (database.isEmpty()) {
            throw RoutingTableProcedureHelpers.databaseNotFoundException(namedDatabaseId.name());
        }
        if (!database.get().getDatabaseAvailabilityGuard().isAvailable()) {
            throw new ProcedureException(
                    DatabaseUnavailable,
                    "Unable to get a routing table for database '" + namedDatabaseId.name()
                            + "' because this database is unavailable");
        }
    }

    Optional<Database> getDatabase(NamedDatabaseId databaseId) {
        return databaseContextProvider.getDatabaseContext(databaseId).map(DatabaseContext::database);
    }
}
