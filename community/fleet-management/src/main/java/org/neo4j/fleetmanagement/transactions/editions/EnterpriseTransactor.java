package org.neo4j.fleetmanagement.transactions.editions;

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

import static org.neo4j.fleetmanagement.common.TransactionUtil.withSystemTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.fleetmanagement.common.CachedMethod;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.topology.model.Database;
import org.neo4j.fleetmanagement.topology.model.Server;
import org.neo4j.fleetmanagement.transactions.AbstractTransactor;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.transactions.model.ResultMap;
import org.neo4j.graphdb.Result;

public class EnterpriseTransactor extends AbstractTransactor implements ITransactor {

    private final CachedMethod<Server.License> licenseCacheMethod = new CachedMethod<>();

    public EnterpriseTransactor(State state) {
        super(state);
    }

    public Map<String, Server> getServers() {
        return withSystemTransaction(databaseManagementService, tx -> {
            try (Result r =
                    tx.execute("SHOW SERVERS YIELD name, state, serverId, address, health, modeConstraint, version")) {
                Map<String, Server> instanceMap = new HashMap<>();
                while (r.hasNext()) {
                    var result = new ResultMap(r.next());
                    var server = new Server();
                    server.serverId = result.getString("serverId");
                    server.name = result.getString("name", null);
                    server.address = result.getString("address", "unknown");
                    server.health = result.getString("health");
                    server.modeConstraint = result.getString("modeConstraint");
                    server.version = result.getString("version", "unknown");
                    server.state = result.getString("state", null);
                    instanceMap.put(server.serverId, server);
                }
                return instanceMap;
            }
        });
    }

    public Map<String, List<Database>> getDatabases() {
        return withSystemTransaction(databaseManagementService, tx -> {
            try (Result r = tx.execute("SHOW DATABASES YIELD *")) {
                Map<String, List<Database>> databasesByInstance = new HashMap<>();
                while (r.hasNext()) {
                    var instanceDatabases = new ResultMap(r.next());
                    var serverId = instanceDatabases.getString("serverID");
                    var dbArray = databasesByInstance.computeIfAbsent(serverId, k -> new ArrayList<>());

                    var oneDb = Shared.getDatabase(instanceDatabases);
                    dbArray.add(oneDb);
                }
                return databasesByInstance;
            }
        });
    }

    public Server.License getLicense() {
        return licenseCacheMethod.GetCachedOrRun(() -> {
            var license = new Server.License();
            license.type = Server.License.LicenseType.COMMERCIAL;

            String status = withSystemTransaction(databaseManagementService, tx -> {
                try (Result r = tx.execute("CALL dbms.licenseAgreementDetails()")) {
                    var result = new ResultMap(r.next());
                    license.daysLeftOnTrial = result.getInteger("daysLeftOnTrial", null);
                    license.totalTrialDays = result.getInteger("totalTrialDays", null);
                    return result.getString("status");
                }
            });

            switch (status) {
                case "yes":
                    license.state = Server.License.LicenseState.VALID;
                    break;
                case "no":
                    license.state = Server.License.LicenseState.NOT_ACCEPTED;
                    license.type = Server.License.LicenseType.EVALUATION;
                    break;
                case "expired":
                    license.state = Server.License.LicenseState.EXPIRED;
                    license.type = Server.License.LicenseType.EVALUATION;
                    break;
                case "eval":
                    license.state = Server.License.LicenseState.VALID;
                    license.type = Server.License.LicenseType.EVALUATION;
                    break;
                default:
                    license.state = Server.License.LicenseState.UNKNOWN;
                    break;
            }

            return license;
        });
    }

    public void setToken(String token) {
        super.setToken(token);
    }

    @Override
    public void rotateToken() {
        super.rotateToken();
    }

    @Override
    public void deleteToken() {
        super.deleteToken();
    }
}
