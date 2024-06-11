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
package org.neo4j.udc;

import static java.lang.String.valueOf;
import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.udc_initial_delay_ms;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.udc_network_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.udc_report_interval_ms;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.udc_enabled;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.api.database.DatabaseSizeService;
import org.neo4j.kernel.diagnostics.providers.PackagingDiagnostics;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.util.StoreIdDecodeUtils;

public class UserDataCollector extends LifecycleAdapter {
    private static final URI UDC_URI = URI.create("https://udc.neo4j.com/server");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String CLUSTER_SIZE_KEY = "clusterSize";

    private final Config config;
    private final Edition edition;
    private final FileSystemAbstraction fs;

    private final Map<String, String> data = new HashMap<>();
    private final Collection<UserDataCollectorSource> additionalData;
    private final DatabaseManagementService databaseManagementService;
    private final JobScheduler jobScheduler;
    private final Log userLog;
    private final InternalLog log;
    private final boolean networkEnabled;

    private long counter;
    private JobHandle<?> jobHandle;

    public UserDataCollector(
            Config config,
            DatabaseManagementService databaseManagementService,
            DbmsInfo dbmsInfo,
            JobScheduler jobScheduler,
            LogProvider logProvider,
            InternalLogProvider internalLogProvider,
            FileSystemAbstraction fs) {
        this.config = config;
        this.databaseManagementService = databaseManagementService;
        this.jobScheduler = jobScheduler;
        this.userLog = logProvider.getLog(UserDataCollector.class);
        this.log = internalLogProvider.getLog(UserDataCollector.class);
        this.edition = dbmsInfo.edition;
        this.fs = fs;
        this.networkEnabled = config.get(udc_network_enabled);

        data.put("edition", dbmsInfo.edition.toString().toLowerCase(Locale.ROOT));
        data.put("numberOfProcessors", valueOf(Runtime.getRuntime().availableProcessors()));
        data.put("totalMemory", valueOf(OsBeanUtil.getTotalPhysicalMemory()));
        data.put("totalHeap", valueOf(Runtime.getRuntime().maxMemory()));
        data.put("version", Version.getNeo4jVersion());
        data.put("packaging", getPackagingInformation(config, fs));
        data.put(CLUSTER_SIZE_KEY, String.valueOf(1)); // Will be overwritten by actual value in enterprise edition

        additionalData = Services.loadAll(UserDataCollectorSource.class);
    }

    // The number of nodes, relationships, labels and properties in the database.

    public void ping() {
        data.put("counter", valueOf(++counter));
        GraphDatabaseAPI systemDatabase =
                (GraphDatabaseAPI) databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        var storeIdProvider = systemDatabase.getDependencyResolver().resolveDependency(StoreIdProvider.class);
        data.put("uuid", StoreIdDecodeUtils.decodeId(storeIdProvider.getExternalStoreId()));
        data.putAll(getDatabaseEntityCount());

        // Merge with additional sources
        for (UserDataCollectorSource source : additionalData) {
            try {
                data.putAll(source.getData(databaseManagementService, fs, config, log));
            } catch (Exception e) {
                log.debug(
                        "Failed to collect data from source "
                                + source.getClass().getName(),
                        e);
            }
        }

        try {
            HttpClient httpClient = HttpClient.newHttpClient();
            String jsonPayload = OBJECT_MAPPER.writeValueAsString(data);
            log.debug("Sending anonymous user data '%s'", jsonPayload);
            if (networkEnabled) {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(UDC_URI)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    log.debug(UDC_URI + " responded with " + response.statusCode());
                }
            }
        } catch (Exception e) {
            log.debug("Unable to send data to " + UDC_URI, e);
        }
    }

    @Override
    public void start() {
        if (edition != Edition.COMMUNITY && edition != Edition.ENTERPRISE) {
            return; // Don't report for admin commands
        }

        if (!config.get(udc_enabled)) {
            return; // Don't report if explicitly disabled
        }

        userLog.info("Anonymous Usage Data is being sent to Neo4j, see https://neo4j.com/docs/usage-data/");

        jobHandle = jobScheduler.scheduleRecurring(
                Group.UDC,
                this::ping,
                config.get(udc_initial_delay_ms),
                config.get(udc_report_interval_ms),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (jobHandle != null) {
            jobHandle.cancel();
        }
    }

    private Map<String, String> getDatabaseEntityCount() {
        GraphDatabaseAPI systemDatabase =
                (GraphDatabaseAPI) databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        DatabaseSizeService databaseSizeService =
                systemDatabase.getDependencyResolver().resolveDependency(DatabaseSizeService.class);
        List<String> databases = databaseManagementService.listDatabases();
        long nodes = 0;
        long relationships = 0;
        long labels = 0;
        long dataSize = 0;
        for (String database : databases) {
            try {
                GraphDatabaseAPI db = (GraphDatabaseAPI) databaseManagementService.database(database);
                StorageEngine storageEngine = db.getDependencyResolver().resolveDependency(StorageEngine.class);
                StoreEntityCounters storeEntityCounters = storageEngine.storeEntityCounters();
                nodes += storeEntityCounters.estimateNodes();
                relationships += storeEntityCounters.estimateRelationships();
                labels += storeEntityCounters.estimateLabels();
                dataSize += databaseSizeService.getDatabaseDataSize(db.databaseId());
            } catch (Exception e) {
                log.debug("Failed to collect data from database " + database, e);
            }
        }
        return Map.of(
                "nodes", String.valueOf(nodes),
                "relationships", String.valueOf(relationships),
                "labels", String.valueOf(labels),
                "storeSize", String.valueOf(dataSize));
    }

    private static String getPackagingInformation(Config config, FileSystemAbstraction fs) {
        Path packagingInfoFile = config.get(neo4j_home).resolve(PackagingDiagnostics.PACKAGING_INFO_FILENAME);
        try {
            List<String> lines = FileSystemUtils.readLines(fs, packagingInfoFile, EmptyMemoryTracker.INSTANCE);
            for (String line : requireNonNull(lines)) {
                if (line.startsWith("Package Type:")) {
                    return line.substring("Package Type:".length()).trim();
                }
            }
        } catch (Exception e) {
            return "error";
        }
        return "unknown";
    }
}
