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
package org.neo4j.kernel.diagnostics.providers;

import static java.lang.String.format;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.neo4j.util.FeatureToggles.getInteger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Consumer;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.HeapDumpDiagnostics;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class DbmsDiagnosticsManager {
    private static final int CONCISE_DATABASE_DUMP_THRESHOLD =
            getInteger(DbmsDiagnosticsManager.class, "conciseDumpThreshold", 10);
    private static final int CONCISE_DATABASE_NAMES_PER_ROW = 5;
    private final Dependencies dependencies;
    private final boolean enabled;
    private final InternalLog internalLog;
    private final Boolean splitIntoSections;
    private final JobScheduler jobScheduler;

    public DbmsDiagnosticsManager(Dependencies dependencies, LogService logService) {
        this.internalLog = logService.getInternalLog(DiagnosticsManager.class);
        this.dependencies = dependencies;
        Config config = dependencies.resolveDependency(Config.class);
        this.enabled = config.get(GraphDatabaseInternalSettings.dump_diagnostics);
        this.splitIntoSections = config.get(GraphDatabaseInternalSettings.split_diagnostics);
        this.jobScheduler = splitIntoSections ? dependencies.resolveDependency(JobScheduler.class) : null;
    }

    public void dumpSystemDiagnostics() {
        if (enabled) {
            dumpSystemDiagnostics(internalLog);
        }
    }

    public void dumpDatabaseDiagnostics(Database database) {
        if (enabled) {
            dumpDatabaseDiagnostics(database, internalLog, false);
        }
    }

    public void dumpAll() {
        dumpAll(internalLog);
    }

    public void dumpAll(InternalLog log) {
        if (enabled) {
            dumpSystemDiagnostics(log);
            dumpAllDatabases(log);
        }
    }

    private void dumpAllDatabases(InternalLog log) {
        Collection<? extends DatabaseContext> values =
                getDatabaseManager().registeredDatabases().values();
        if (values.size() > CONCISE_DATABASE_DUMP_THRESHOLD) {
            dumpConciseDiagnostics(values, log);
        } else {
            values.stream()
                    .flatMap(ctx -> ctx.optionalDatabase().stream())
                    .forEach(db -> dumpDatabaseDiagnostics(db, log, true));
        }
    }

    private void dumpConciseDiagnostics(Collection<? extends DatabaseContext> databaseContexts, InternalLog log) {
        var startedDbs = databaseContexts.stream()
                .flatMap(ctx -> ctx.optionalDatabase().stream())
                .filter(Database::isStarted)
                .collect(toList());
        var stoppedDbs = databaseContexts.stream()
                .flatMap(ctx -> ctx.optionalDatabase().stream())
                .filter(not(Database::isStarted))
                .collect(toList());

        dumpAsSingleMessage(log, diagnosticsLogger -> {
            logDatabasesState(diagnosticsLogger, startedDbs, "Started");
            diagnosticsLogger.newSegment();
            logDatabasesState(diagnosticsLogger, stoppedDbs, "Stopped");
        });
    }

    private void logDatabasesState(ExtendedDiagnosticsLogger log, List<Database> databases, String state) {
        DiagnosticsManager.section(log, state + " Databases");
        if (databases.isEmpty()) {
            log.log(format("There are no %s databases", state.toLowerCase()));
            return;
        }
        int lastIndex = 0;
        for (int i = CONCISE_DATABASE_NAMES_PER_ROW; i < databases.size(); i += CONCISE_DATABASE_NAMES_PER_ROW) {
            var subList = databases.subList(lastIndex, i);
            logDatabases(log, subList);
            log.newSegment();
            lastIndex = i;
        }
        var lastDbs = databases.subList(lastIndex, databases.size());
        log.newSegment();
        logDatabases(log, lastDbs);
    }

    private void logDatabases(DiagnosticsLogger log, List<Database> subList) {
        log.log(subList.stream()
                .map(database -> database.getNamedDatabaseId().name())
                .collect(joining(", ")));
    }

    private void dumpSystemDiagnostics(InternalLog log) {
        dumpAsSingleMessage(log, diagnosticsLogger -> {
            Config config = dependencies.resolveDependency(Config.class);

            DiagnosticsManager.section(diagnosticsLogger, "System diagnostics");
            diagnosticsLogger.newSegment();
            for (SystemDiagnostics diagnostics : SystemDiagnostics.values()) {
                DiagnosticsManager.dump(diagnostics, log, diagnosticsLogger);
                diagnosticsLogger.newSegment();
            }
            diagnosticsLogger.newSegment();
            DiagnosticsManager.dump(new ConfigDiagnostics(config), log, diagnosticsLogger);
            diagnosticsLogger.newSegment();
            DiagnosticsManager.dump(new PackagingDiagnostics(config), log, diagnosticsLogger);
            diagnosticsLogger.newSegment();
            // dump any custom additional diagnostics that can be registered by specific edition
            dependencies
                    .resolveTypeDependencies(DiagnosticsProvider.class)
                    .forEach(provider -> DiagnosticsManager.dump(provider, log, diagnosticsLogger));
            diagnosticsLogger.newSegment();
        });
    }

    private void dumpDatabaseDiagnostics(Database database, InternalLog log, boolean checkStatus) {
        dumpAsSingleMessageWithDbPrefix(
                log,
                diagnosticsLogger -> {
                    dumpDatabaseSectionName(database, diagnosticsLogger);
                    if (checkStatus) {
                        logDatabaseStatus(database, diagnosticsLogger);
                        diagnosticsLogger.newSegment();
                        if (!database.isStarted()) {
                            return;
                        }
                    }
                    DependencyResolver databaseResolver = database.getDependencyResolver();
                    DbmsInfo dbmsInfo = databaseResolver.resolveDependency(DbmsInfo.class);
                    FileSystemAbstraction fs = databaseResolver.resolveDependency(FileSystemAbstraction.class);
                    StorageEngineFactory storageEngineFactory =
                            databaseResolver.resolveDependency(StorageEngineFactory.class);
                    DeviceMapper deviceMapper = databaseResolver.resolveDependency(DeviceMapper.class);
                    StorageEngine storageEngine = databaseResolver.resolveDependency(StorageEngine.class);

                    DiagnosticsManager.dump(
                            new VersionDiagnostics(dbmsInfo, database.getStoreId()), log, diagnosticsLogger);
                    diagnosticsLogger.newSegment();
                    DiagnosticsManager.dump(
                            new StoreFilesDiagnostics(
                                    storageEngineFactory, fs, database.getDatabaseLayout(), deviceMapper),
                            log,
                            diagnosticsLogger);
                    diagnosticsLogger.newSegment();
                    DiagnosticsManager.dump(new TransactionRangeDiagnostics(database), log, diagnosticsLogger);
                    diagnosticsLogger.newSegment();
                    storageEngine.dumpDiagnostics(log, diagnosticsLogger);
                    diagnosticsLogger.newSegment();
                },
                database.getNamedDatabaseId());
    }

    private void dumpAsSingleMessageWithDbPrefix(
            InternalLog log, Consumer<ExtendedDiagnosticsLogger> dumpFunction, NamedDatabaseId db) {
        dumpAsSingleMessageWithPrefix(log, dumpFunction, "[" + db.logPrefix() + "] ");
    }

    private void dumpAsSingleMessage(InternalLog log, Consumer<ExtendedDiagnosticsLogger> dumpFunction) {
        dumpAsSingleMessageWithPrefix(log, dumpFunction, "");
    }

    /**
     * Messages will be buffered and logged as one single message to make sure that diagnostics are grouped together in the log.
     */
    private void dumpAsSingleMessageWithPrefix(
            InternalLog log, Consumer<ExtendedDiagnosticsLogger> dumpFunction, String prefix) {
        // Optimization to skip diagnostics dumping (which is time consuming) if there's no log anyway.
        // This is first and foremost useful for speeding up testing.
        if (log == NullLog.getInstance()) {
            return;
        }
        ExtendedDiagnosticsLogger diagnosticsLogger = new ExtendedDiagnosticsLogger(prefix);
        dumpFunction.accept(diagnosticsLogger);

        String message = diagnosticsLogger.asMessage();
        HeapDumpDiagnostics.addDiagnostics(prefix, message);

        if (splitIntoSections) {
            List<String> segments = diagnosticsLogger.asSegments();
            List<String> chunksToLog = new ArrayList<>();
            for (String segment : segments) {
                // Segments can be long. Allow at most 50 lines per message
                String[] split = segment.split(System.lineSeparator());
                int chunkSize = 50;
                if (split.length <= chunkSize) {
                    chunksToLog.add(segment);
                } else {
                    for (int i = 0; i < split.length; i += chunkSize) {
                        String[] chunk = Arrays.copyOfRange(split, i, Math.min(i + chunkSize, split.length));
                        chunksToLog.add(String.join(System.lineSeparator(), chunk));
                    }
                }
            }
            jobScheduler.schedule(Group.LOG_ROTATION, () -> {
                // Synchronize here avoid risk of interleaving sections with other concurrent diagnostic messages
                synchronized (DbmsDiagnosticsManager.this) {
                    chunksToLog.forEach(log::info);
                }
            });
        } else {
            log.info(message);
        }
    }

    private static class ExtendedDiagnosticsLogger implements DiagnosticsLogger {
        private final StringJoiner messages;
        private final List<Integer> segmentIndexes = new ArrayList<>();

        ExtendedDiagnosticsLogger(String prefix) {
            this.messages = new StringJoiner(
                    System.lineSeparator() + " ".repeat(64) + prefix,
                    prefix + System.lineSeparator() + " ".repeat(64) + prefix,
                    "");
        }

        @Override
        public void log(String logMessage) {
            messages.add(logMessage);
        }

        void newSegment() {
            int lastIndex = segmentIndexes.isEmpty() ? 0 : segmentIndexes.get(segmentIndexes.size() - 1);
            int currIndex = messages.length();
            if (currIndex > lastIndex) {
                segmentIndexes.add(currIndex);
            }
        }

        String asMessage() {
            return messages.toString();
        }

        List<String> asSegments() {
            String message = asMessage();
            List<String> segments = new ArrayList<>();
            int prevIndex = 0;
            for (int index : segmentIndexes) {
                segments.add(message.substring(prevIndex, index));
                prevIndex = index;
            }
            String lastMessage = message.substring(prevIndex);
            if (!lastMessage.isEmpty()) {
                segments.add(lastMessage);
            }
            return segments;
        }
    }

    private static void logDatabaseStatus(Database database, DiagnosticsLogger log) {
        log.log(format("Database is %s.", database.isStarted() ? "started" : "stopped"));
    }

    private static void dumpDatabaseSectionName(Database database, DiagnosticsLogger log) {
        DiagnosticsManager.section(
                log, "Database: " + database.getNamedDatabaseId().name());
    }

    private DatabaseContextProvider<?> getDatabaseManager() {
        return dependencies.resolveDependency(DatabaseContextProvider.class);
    }
}
