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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.database.DatabaseTracers.EMPTY;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.Command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.ReadOnlyTransactionIdStore;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "info",
        header = "Print information about a Neo4j database store.",
        description = "Print information about a Neo4j database store, such as what version of Neo4j created it.")
public class StoreInfoCommand extends AbstractAdminCommand {
    private static final String PLAIN_FORMAT = "text";
    private static final String JSON_FORMAT = "json";

    @Parameters(
            arity = "0..1",
            paramLabel = "<database>",
            defaultValue = "*",
            description = "Name of the database to show info for. Can contain * and ? for globbing. "
                    + "Note that * and ? have special meaning in some shells "
                    + "and might need to be escaped or used with quotes.",
            converter = Converters.DatabaseNamePatternConverter.class)
    private DatabaseNamePattern database;

    @Option(
            names = "--format",
            arity = "1",
            defaultValue = "text",
            description = "The format of the returned information.",
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
            paramLabel = "text|json",
            converter = FormatConverter.class)
    private boolean structuredFormat;

    @Option(names = "--from-path", description = "Path to databases directory.")
    private Path path;

    private final StorageEngineFactory.Selector storageEngineSelector;

    public StoreInfoCommand(ExecutionContext ctx) {
        this(ctx, StorageEngineFactory.SELECTOR);
    }

    StoreInfoCommand(ExecutionContext ctx, StorageEngineFactory.Selector storageEngineSelector) {
        super(ctx);
        this.storageEngineSelector = storageEngineSelector;
    }

    static class FormatConverter implements CommandLine.ITypeConverter<Boolean> {
        @Override
        public Boolean convert(String name) {
            String lowerCase = name.toLowerCase(Locale.ROOT);
            return switch (lowerCase) {
                case PLAIN_FORMAT, FALSE -> false;
                case JSON_FORMAT, TRUE -> true;
                default -> throw new CommandLine.TypeConversionException(
                        format("Invalid format '%s'. Supported options are 'text' or 'json'", name));
            };
        }
    }

    @Override
    public void execute() {
        var config = createConfig();
        var neo4jLayout = Neo4jLayout.of(config);
        try (var fs = ctx.fs();
                var jobScheduler = createInitialisedScheduler();
                var pageCache = StandalonePageCacheFactory.createPageCache(fs, jobScheduler, PageCacheTracer.NULL)) {

            validateDatabasesPath(fs, neo4jLayout.databasesDirectory());
            if (database.containsPattern()) {
                var collector = structuredFormat
                        ? Collectors.joining(",", "[", "]")
                        : Collectors.joining(System.lineSeparator() + System.lineSeparator());
                var result = Arrays.stream(fs.listFiles(neo4jLayout.databasesDirectory()))
                        .sorted(comparing(Path::getFileName))
                        .map(dbPath ->
                                neo4jLayout.databaseLayout(dbPath.getFileName().toString()))
                        .filter(dbLayout -> database.matches(dbLayout.getDatabaseName())
                                && Validators.isExistingDatabase(storageEngineSelector, fs, dbLayout))
                        .map(dbLayout -> printInfo(fs, dbLayout, pageCache, config, structuredFormat, true))
                        .collect(collector);
                ctx.out().println(result);
            } else {
                var databaseLayout = neo4jLayout.databaseLayout(database.getDatabaseName());
                if (!Validators.isExistingDatabase(storageEngineSelector, fs, databaseLayout)) {
                    throw new CommandFailedException(format(
                            "Database does not exist: '%s'. Directory '%s' does not contain a database",
                            database.getDatabaseName(), databaseLayout.databaseDirectory()));
                }
                ctx.out().println(printInfo(fs, databaseLayout, pageCache, config, structuredFormat, false));
            }
        } catch (CommandFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new CommandFailedException(format("Failed to execute command: '%s'.", e.getMessage()), e);
        }
    }

    private Config createConfig() {
        final var builder = createPrefilledConfigBuilder().set(GraphDatabaseSettings.read_only_database_default, true);
        if (path != null) {
            builder.set(
                    GraphDatabaseInternalSettings.databases_root_path,
                    path.toAbsolutePath().normalize());
        }
        return builder.build();
    }

    private void validateDatabasesPath(FileSystemAbstraction fs, Path databasesPath) {
        if (!fs.isDirectory(databasesPath)) {
            throw new IllegalArgumentException(format("Provided path %s must point to a directory.", databasesPath));
        }

        // check not a path to store files
        final var databaseLayout = DatabaseLayout.ofFlat(databasesPath);
        if (Validators.isExistingDatabase(storageEngineSelector, fs, databaseLayout)) {
            throw new IllegalArgumentException(format(
                    "The directory %s contains the store files of a single database."
                            + " --from-path should point to the databases directory.",
                    databasesPath));
        }
    }

    private String printInfo(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            PageCache pageCache,
            Config config,
            boolean structured,
            boolean failSilently) {
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        try (var ignored = LockChecker.checkDatabaseLock(databaseLayout);
                var cursorContext = NULL_CONTEXT_FACTORY.create("printInfo")) {
            var storageEngineFactory = storageEngineSelector
                    .selectStorageEngine(fs, databaseLayout)
                    .orElseThrow();
            StoreId storeId = storageEngineFactory.retrieveStoreId(fs, databaseLayout, pageCache, cursorContext);
            if (storeId == null) {
                throw new CommandFailedException(
                        format("Could not find version metadata in store '%s'", databaseLayout.databaseDirectory()));
            }
            var versionInformation =
                    storageEngineFactory.versionInformation(storeId).orElseThrow();
            var logTail = getLogTail(fs, databaseLayout, config, memoryTracker, storageEngineFactory);
            var recoveryRequired =
                    checkRecoveryState(fs, pageCache, databaseLayout, config, memoryTracker, storageEngineFactory);
            var txIdStore = new ReadOnlyTransactionIdStore(logTail);
            var lastTxId =
                    txIdStore.getLastCommittedTransactionId(); // Latest committed tx id found in metadata store. May be
            // behind
            // if recovery is required.
            var successorVersion =
                    versionInformation.successorStoreVersion(config).orElse(null);
            var storeInfo = StoreInfo.notInUseResult(
                    databaseLayout.getDatabaseName(), versionInformation, successorVersion, lastTxId, recoveryRequired);

            return storeInfo.print(structured);
        } catch (FileLockException e) {
            if (!failSilently) {
                throw new CommandFailedException(
                        format(
                                "Failed to execute command as the database '%s' is in use. "
                                        + "Please stop it and try again.",
                                databaseLayout.getDatabaseName()),
                        e);
            }
            return StoreInfo.inUseResult(databaseLayout.getDatabaseName()).print(structured);
        } catch (CommandFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new CommandFailedException(format("Failed to execute command: '%s'.", e.getMessage()), e);
        }
    }

    private LogTailMetadata getLogTail(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            MemoryTracker memoryTracker,
            StorageEngineFactory storageEngineFactory)
            throws IOException {
        LogTailExtractor logTailExtractor = new LogTailExtractor(fs, config, storageEngineFactory, EMPTY);
        return logTailExtractor.getTailMetadata(databaseLayout, memoryTracker);
    }

    private static boolean checkRecoveryState(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            Config config,
            MemoryTracker memoryTracker,
            StorageEngineFactory storageEngineFactory) {
        try {
            return isRecoveryRequired(
                    fs,
                    pageCache,
                    databaseLayout,
                    storageEngineFactory,
                    config,
                    Optional.empty(),
                    memoryTracker,
                    EMPTY);
        } catch (Exception e) {
            throw new CommandFailedException(
                    format("Failed to execute command when checking for recovery state: '%s'.", e.getMessage()), e);
        }
    }

    private record StoreInfo(
            String databaseName,
            StoreVersion currentStoreVersion,
            StoreVersion successorStoreVersion,
            long lastCommittedTransaction,
            boolean recoveryRequired,
            boolean inUse) {
        static StoreInfo inUseResult(String databaseName) {
            return new StoreInfo(databaseName, null, null, -1, true, true);
        }

        static StoreInfo notInUseResult(
                String databaseName,
                StoreVersion currentStoreVersion,
                StoreVersion successorStoreVersion,
                long lastCommittedTransaction,
                boolean recoveryRequired) {
            return new StoreInfo(
                    databaseName,
                    currentStoreVersion,
                    successorStoreVersion,
                    lastCommittedTransaction,
                    recoveryRequired,
                    false);
        }

        List<StoreInfoField> printFields() {
            return List.of(
                    new StoreInfoField(InfoType.DatabaseName, databaseName),
                    new StoreInfoField(InfoType.InUse, Boolean.toString(inUse)),
                    new StoreInfoField(
                            InfoType.StoreFormat,
                            currentStoreVersion != null ? currentStoreVersion.getStoreVersionUserString() : null),
                    new StoreInfoField(
                            InfoType.StoreFormatIntroduced,
                            currentStoreVersion != null ? currentStoreVersion.introductionNeo4jVersion() : null),
                    new StoreInfoField(
                            InfoType.StoreFormatSuperseded,
                            successorStoreVersion != null ? successorStoreVersion.introductionNeo4jVersion() : null),
                    new StoreInfoField(InfoType.LastCommittedTransaction, Long.toString(lastCommittedTransaction)),
                    new StoreInfoField(InfoType.RecoveryRequired, Boolean.toString(recoveryRequired)));
        }

        String print(boolean structured) {
            if (!structured) {
                return printFields().stream()
                        .filter(p -> Objects.nonNull(p.value()))
                        .map(p -> p.type().justifiedPretty(p.value()))
                        .collect(Collectors.joining(System.lineSeparator()));
            }
            return printFields().stream()
                    .map(p -> p.type().structuredJson(p.value()))
                    .collect(Collectors.joining(",", "{", "}"));
        }

        private record StoreInfoField(InfoType type, String value) {}
    }

    private enum InfoType {
        InUse("Database in use", "inUse"),
        DatabaseName("Database name", "databaseName"),
        StoreFormat("Store format version", "storeFormat"),
        StoreFormatIntroduced("Store format introduced in", "storeFormatIntroduced"),
        StoreFormatSuperseded("Store format superseded in", "storeFormatSuperseded"),
        LastCommittedTransaction("Last committed transaction id", "lastCommittedTransaction"),
        RecoveryRequired("Store needs recovery", "recoveryRequired");

        private final String prettyPrint;
        private final String jsonKey;

        InfoType(String prettyPrint, String jsonKey) {
            this.prettyPrint = prettyPrint;
            this.jsonKey = jsonKey;
        }

        String justifiedPretty(String value) {
            var nullSafeValue = value == null ? "N/A" : value;
            var leftJustifiedFmt = "%-30s%s";
            return String.format(leftJustifiedFmt, prettyPrint + ":", nullSafeValue);
        }

        String structuredJson(String value) {
            var kFmt = "\"%s\":";
            var kvFmt = kFmt + "\"%s\"";
            return value == null ? String.format(kFmt + "null", jsonKey) : String.format(kvFmt, jsonKey, value);
        }
    }
}
