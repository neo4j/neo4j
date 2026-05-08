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
package org.neo4j.fleetmanagement.transactions;

import static org.neo4j.fleetmanagement.common.TransactionUtil.withSystemTransaction;
import static org.neo4j.fleetmanagement.common.TransactionUtil.withTransaction;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fleetmanagement.common.CachedMethod;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.topology.model.GraphCount;
import org.neo4j.fleetmanagement.topology.model.Server;
import org.neo4j.fleetmanagement.transactions.model.ResultMap;
import org.neo4j.fleetmanagement.transactions.model.VersionAndEdition;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;

public abstract class AbstractTransactor {
    public static final String TOKEN_ROTATION_STATE_INDICATOR = "ROTATING";
    protected DatabaseManagementService databaseManagementService;
    protected Log log;
    protected final State state;

    private final CachedMethod<Server.License> bloomLicenseCache = new CachedMethod<>();
    private final CachedMethod<Server.License> gdsLicenseCache = new CachedMethod<>();

    protected AbstractTransactor(State state) {
        this.state = state;
    }

    public void init(DatabaseManagementService databaseManagementService) {
        this.databaseManagementService = databaseManagementService;
        this.log = Logger.getNeo4jLogger();
    }

    public boolean getTokenStatus() {
        return withSystemTransaction(databaseManagementService, tx -> {
            try (var rs = tx.findNodes(Label.label("FleetManagementConfiguration"))) {
                Optional<Node> maybeNode = rs.stream().findFirst();
                return maybeNode.map(node -> node.hasProperty("token")).orElse(false);
            }
        });
    }

    public boolean getTokenRotationStatus() {
        return withSystemTransaction(databaseManagementService, tx -> {
            try (var rs = tx.findNodes(Label.label("FleetManagementConfiguration"))) {
                Optional<Node> maybeNode = rs.stream().findFirst();
                return maybeNode
                        .map(node -> node.hasProperty("token")
                                && node.getProperty("token").equals(TOKEN_ROTATION_STATE_INDICATOR))
                        .orElse(false);
            }
        });
    }

    public String getToken() {
        return withSystemTransaction(databaseManagementService, tx -> {
            try (var rs = tx.findNodes(Label.label("FleetManagementConfiguration"))) {
                Optional<Node> maybeNode = rs.stream().findFirst();
                return maybeNode
                        .map(node -> node.getProperty("token").toString())
                        .orElse(null);
            }
        });
    }

    public VersionAndEdition getVersionAndEdition() {
        return withSystemTransaction(databaseManagementService, tx -> {
            var versionAndEdition = new VersionAndEdition();
            try (var r = tx.execute("CALL dbms.components() YIELD name, versions, edition")) {
                while (r.hasNext()) {
                    Map<String, Object> resultMap = r.next();
                    var name = resultMap.get("name").toString();
                    if (!name.equals("Neo4j Kernel")) {
                        continue;
                    }

                    versionAndEdition.edition = resultMap.get("edition").toString();
                    try {
                        var versions = (List<String>) resultMap.get("versions");
                        versionAndEdition.version = versions.getFirst();
                    } catch (ClassCastException e) {
                        this.log.error("Type casting failed: " + e.getMessage());
                    }
                    return versionAndEdition;
                }
            }
            return versionAndEdition;
        });
    }

    public GraphCount getGraphCount(String databaseName) {
        return withTransaction(databaseManagementService, databaseName, tx -> {
            try (var r = tx.execute("CALL db.stats.retrieve(\"GRAPH COUNTS\")")) {
                while (r.hasNext()) {
                    var result = r.next();
                    if (result.get("section").equals("GRAPH COUNTS")) {
                        var graphCount = new GraphCount();
                        var data = result.get("data");
                        if (data != null) {
                            if (data instanceof Map) {
                                graphCount.relationshipCount = getCount(((Map<?, ?>) data).get("relationships"));
                                graphCount.nodeCount = getCount(((Map<?, ?>) data).get("nodes"));
                            }
                        }
                        return graphCount;
                    }
                }
            }
            return null;
        });
    }

    private Long getCount(Object listOfCounts) {
        if (listOfCounts instanceof List) {
            if (((List<?>) listOfCounts).isEmpty()) {
                return null;
            }
            for (Object count : (List<?>) listOfCounts) {
                if (count instanceof Map) {
                    if (((Map<?, ?>) count).size() > 1) {
                        // Only want the count with no labels, continue
                        continue;
                    }
                    var countValue = ((Map<?, ?>) count).get("count");
                    return countValue != null ? Long.parseLong(countValue.toString()) : null;
                }
            }
        }
        return null;
    }

    protected void setToken(String token) {
        withSystemTransaction(databaseManagementService, tx -> {
            try (var rs = tx.findNodes(Label.label("FleetManagementConfiguration"))) {
                Optional<Node> maybeNode = rs.stream().findFirst();
                Node node = maybeNode.orElseGet(() -> tx.createNode(Label.label("FleetManagementConfiguration")));

                node.setProperty("token", token);
            }
            tx.commit();
        });
    }

    protected void rotateToken() {
        withSystemTransaction(databaseManagementService, tx -> {
            try (var rs = tx.findNodes(Label.label("FleetManagementConfiguration"))) {
                Optional<Node> maybeNode = rs.stream().findFirst();
                maybeNode.ifPresent(node -> node.setProperty("token", TOKEN_ROTATION_STATE_INDICATOR));
            }
            tx.commit();
        });
    }

    protected void deleteToken() {
        withSystemTransaction(databaseManagementService, tx -> {
            try (var rs = tx.findNodes(Label.label("FleetManagementConfiguration"))) {
                Optional<Node> maybeNode = rs.stream().findFirst();
                maybeNode.ifPresent(Node::delete);
            }
            tx.commit();

            this.state.setActive(false);
        });
    }

    public Server.License getBloomLicense() {
        return bloomLicenseCache.GetCachedOrRun(() -> {
            Boolean bloomInstalled = withSystemTransaction(databaseManagementService, tx -> {
                try (var r = tx.execute("SHOW PROCEDURES YIELD name WHERE name = 'bloom.checkLicenseCompliance'")) {
                    return r.hasNext();
                }
            });
            if (!bloomInstalled) {
                return null;
            }

            var license = new Server.License();
            license.state = Server.License.LicenseState.UNKNOWN;
            license.type = Server.License.LicenseType.UNSUPPORTED;

            return withSystemTransaction(databaseManagementService, tx -> {
                try (var r = tx.execute("call bloom.checkLicenseCompliance() yield message, status, daysLeft")) {
                    if (r.hasNext()) {
                        var result = new ResultMap(r.next());
                        String status = result.getString("status");
                        license.daysLeftOnTrial = result.getInteger("daysLeft", null);

                        switch (status.toLowerCase()) {
                            case "valid":
                                license.state = Server.License.LicenseState.VALID;
                                license.type = Server.License.LicenseType.COMMERCIAL;
                                break;
                            case "missing":
                                license.state = Server.License.LicenseState.NOT_ACCEPTED;
                                license.type = Server.License.LicenseType.COMMERCIAL;
                                break;
                            case "expired":
                                license.state = Server.License.LicenseState.EXPIRED;
                                license.type = Server.License.LicenseType.COMMERCIAL;
                                break;
                            case "not_accepted":
                                license.state = Server.License.LicenseState.NOT_ACCEPTED;
                                license.type = Server.License.LicenseType.COMMERCIAL;
                                break;
                            default:
                                break;
                        }
                    }

                    return license;
                }
            });
        });
    }

    public Server.License getGdsLicense() {
        return gdsLicenseCache.GetCachedOrRun(() -> {
            Boolean gdsInstalled = withSystemTransaction(databaseManagementService, tx -> {
                try (var r = tx.execute("SHOW PROCEDURES YIELD name WHERE name = 'gds.debug.sysInfo'")) {
                    return r.hasNext();
                }
            });
            if (!gdsInstalled) {
                return null;
            }

            var license = new Server.License();
            license.state = Server.License.LicenseState.UNKNOWN;
            license.type = Server.License.LicenseType.UNSUPPORTED;

            return withSystemTransaction(databaseManagementService, tx -> {
                try (Result r = tx.execute("CALL gds.debug.sysInfo() YIELD key, value")) {

                    String edition = "Unlicensed";
                    String errorMsg = "";
                    ZonedDateTime expirationTime = null;

                    while (r.hasNext()) {
                        var result = new ResultMap(r.next());
                        String key = result.getString("key");

                        switch (key) {
                            case "gdsLicenseError":
                                errorMsg = result.getString("value");
                                break;
                            case "gdsEdition":
                                edition = result.getString("value");
                                break;
                            case "gdsLicenseExpirationTime":
                                try {
                                    expirationTime = result.getZonedDateTime("value");
                                } catch (Exception e) {
                                    log.error("Unable to parse gdsLicenseExpirationTime: " + e.getMessage());
                                }
                                break;
                        }
                    }

                    // Set license properties based on the retrieved information
                    if (!errorMsg.isEmpty()) {
                        log.warn("GDS license error: " + errorMsg);
                    }
                    if (edition.equalsIgnoreCase("licensed")) {
                        license.type = Server.License.LicenseType.COMMERCIAL;
                        license.state = Server.License.LicenseState.VALID;
                    } else if (edition.equalsIgnoreCase("unlicensed")) {
                        license.type = Server.License.LicenseType.FREE;
                        license.state = Server.License.LicenseState.VALID;
                    } else if (edition.equalsIgnoreCase("invalid")) {
                        license.type = Server.License.LicenseType.COMMERCIAL;
                        license.state = Server.License.LicenseState.INVALID;
                    }

                    if (expirationTime != null) {
                        java.time.ZonedDateTime now = java.time.ZonedDateTime.now();
                        long daysLeft = java.time.temporal.ChronoUnit.DAYS.between(now, expirationTime);
                        license.daysLeftOnTrial = (int) daysLeft;

                        if (daysLeft <= 0) {
                            license.state = Server.License.LicenseState.EXPIRED;
                        }
                    }

                    return license;
                }
            });
        });
    }
}
