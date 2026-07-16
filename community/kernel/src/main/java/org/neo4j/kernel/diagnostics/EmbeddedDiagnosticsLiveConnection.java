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
package org.neo4j.kernel.diagnostics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 * In-process {@link DiagnosticsLiveConnection} backed by a {@link DatabaseManagementService}. Queries are executed
 * directly against the embedded databases of the running instance, so - unlike a driver/Bolt based connection - it
 * needs neither a network connection nor authentication. This is what lets a running instance (e.g. fleet management)
 * collect the authenticated diagnostics classifiers about itself.
 * <p>
 * The connection does not own the {@link DatabaseManagementService}; {@link #close()} is therefore a no-op.
 */
public class EmbeddedDiagnosticsLiveConnection implements DiagnosticsLiveConnection {
    private final DatabaseManagementService dbms;
    private final String defaultDatabase;

    /**
     * @param dbms the management service of the running instance.
     * @param defaultDatabase the database to run against when {@code execute} is called with a {@code null} database.
     */
    public EmbeddedDiagnosticsLiveConnection(DatabaseManagementService dbms, String defaultDatabase) {
        this.dbms = dbms;
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    public DiagnosticsQueryResult execute(String database, String query) {
        String target = database != null ? database : defaultDatabase;
        GraphDatabaseService db = dbms.database(target);
        try (Transaction tx = db.beginTx();
                Result result = tx.execute(query)) {
            List<String> columns = List.copyOf(result.columns());
            List<Map<String, Object>> rows = new ArrayList<>();
            while (result.hasNext()) {
                // Materialize each record into a plain map while the transaction is open, so the result can be
                // rendered after it closes.
                rows.add(new LinkedHashMap<>(result.next()));
            }
            return new DiagnosticsQueryResult(columns, rows);
        }
    }

    @Override
    public void close() {
        // The management service is owned by the running instance, not by this connection.
    }
}
