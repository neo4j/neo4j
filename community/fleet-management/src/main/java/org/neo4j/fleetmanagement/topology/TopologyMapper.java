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
package org.neo4j.fleetmanagement.topology;

import static org.neo4j.function.Predicates.notNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.topology.model.Database;
import org.neo4j.fleetmanagement.topology.model.Dbms;
import org.neo4j.fleetmanagement.topology.model.Server;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.transactions.model.VersionAndEdition;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

public class TopologyMapper {
    private final Config config;
    private final FileSystemAbstraction fs;
    private final ITransactor transactor;
    private final ServerIdentity serverIdentity;
    private final Log userLog;
    private final Logger fleetManagerLog;

    private VersionAndEdition versionAndEdition;
    private List<Server.Plugin> plugins;

    private static String dbmsId = null;

    public TopologyMapper(
            Config config, FileSystemAbstraction fs, ITransactor transactor, ServerIdentity serverIdentity) {
        this.config = config;
        this.fs = fs;
        this.transactor = transactor;
        this.serverIdentity = serverIdentity;
        this.userLog = Logger.getNeo4jLogger();
        this.fleetManagerLog = Logger.getFleetManagerLogger();
    }

    public Dbms mapTopology() {
        Dbms dbms = new Dbms();
        dbms.packaging = PackagingInformation.getPackaging(config, fs, this.userLog);
        dbms.serverId = serverIdentity.serverId().uuid().toString();

        // This cannot change during the lifetime of the mapper
        if (this.versionAndEdition == null) {
            this.versionAndEdition = this.transactor.getVersionAndEdition();
        }
        dbms.edition = versionAndEdition.edition;

        if (this.plugins == null) {
            this.plugins = getPlugins(config);
        }

        var instanceMap = transactor.getServers();

        var databasesByInstance = transactor.getDatabases();

        dbms.dbmsId = getDbmsId(() -> databasesByInstance);

        // Add all databases to their respective server
        databasesByInstance.forEach((serverId, dbArray) -> {
            if (!instanceMap.containsKey(serverId)) {
                userLog.warn("ServerId %s reported by database but not found in cluster", serverId);
                return;
            }
            instanceMap.get(serverId).databases = dbArray;
        });
        dbms.servers = new ArrayList<>(instanceMap.values());

        var thisServer = getThisServer(dbms);
        thisServer.plugins = this.plugins;
        thisServer.license = transactor.getLicense();

        try {
            thisServer.bloomLicense = transactor.getBloomLicense();
            thisServer.gdsLicense = transactor.getGdsLicense();
        } catch (Exception e) {
            userLog.warn("Failed to get optional license information: " + e.getMessage());
        }

        dbms.databases = databasesByInstance.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream())
                .map(entry -> entry.name)
                .distinct()
                .collect(Collectors.toList());

        thisServer.databases.forEach(db -> {
            if (!db.name.equals("system") && !db.isComposite() && db.currentStatus.equals("online")) {
                db.graphCount = transactor.getGraphCount(db.name);
            }
        });

        return dbms;
    }

    public String getServerId() {
        return serverIdentity.serverId().uuid().toString();
    }

    public String getServerVersion() {
        // This cannot change during the lifetime of the mapper
        if (this.versionAndEdition == null) {
            this.versionAndEdition = this.transactor.getVersionAndEdition();
        }

        return this.versionAndEdition.version;
    }

    public static String getDbmsId(Supplier<Map<String, List<Database>>> databasesByInstanceSupplier) {
        if (dbmsId != null) {
            return dbmsId;
        }
        dbmsId = databasesByInstanceSupplier.get().values().stream()
                .flatMap(List::stream)
                .filter(db -> Objects.equals(db.name, "system"))
                .map(db -> db.databaseId)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        return dbmsId;
    }

    public boolean isSystemDbWriter(String serverId) {
        var databasesByServer = transactor.getDatabases();
        return databasesByServer.getOrDefault(serverId, List.of()).stream()
                .anyMatch(db -> db.name.equals("system") && db.writer);
    }

    private Server getThisServer(Dbms dbms) {
        for (var server : dbms.servers) {
            if (server.serverId.equals(dbms.serverId)) {
                return server;
            }
        }
        throw new RuntimeException("Could not find this server in the topology");
    }

    private List<Server.Plugin> getPlugins(Config config) {
        var plugins = new ArrayList<Server.Plugin>();

        try (Stream<Path> list = Files.list(config.get(GraphDatabaseSettings.plugin_dir))) {
            List<Path> jarPaths = list.filter(path -> path.toString().endsWith(".jar"))
                    .filter(notNull())
                    .toList();

            for (Path path : jarPaths) {
                try (JarFile jarFile = new JarFile(path.toFile())) {
                    var plugin = new Server.Plugin();
                    plugins.add(plugin);
                    plugin.filename = path.getFileName().toString();

                    fleetManagerLog.debug("Found plugin: %s", path.getFileName());
                    var manifest = jarFile.getManifest();
                    if (manifest == null) {
                        fleetManagerLog.debug("No manifest found for plugin: %s", path.getFileName());
                        continue;
                    }

                    var attrs = manifest.getMainAttributes();
                    plugin.name = attrs.getValue("Implementation-Title");
                    plugin.version = attrs.getValue("Implementation-Version");
                    plugin.vendor = attrs.getValue("Implementation-Vendor");
                } catch (IOException e) {
                    userLog.error("Failed to process plugin " + path.getFileName(), e);
                }
            }

            return plugins;
        } catch (IOException e) {
            userLog.error("Failed to read plugin directory", e);
        }

        return plugins;
    }
}
