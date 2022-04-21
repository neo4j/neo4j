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

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.NamedDatabaseId;

public abstract class BaseRoutingTableProcedureValidator implements RoutingTableProcedureValidator {
    protected final DatabaseManager<?> databaseManager;

    protected BaseRoutingTableProcedureValidator(DatabaseManager<?> databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    @SuppressWarnings("ReturnValueIgnored")
    public void assertDatabaseExists(NamedDatabaseId namedDatabaseId) throws ProcedureException {
        databaseManager
                .databaseIdRepository()
                .getById(namedDatabaseId.databaseId())
                .orElseThrow(() -> RoutingTableProcedureHelpers.databaseNotFoundException(namedDatabaseId.name()));
    }
}
