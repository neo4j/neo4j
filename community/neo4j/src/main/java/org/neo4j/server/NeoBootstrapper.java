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
package org.neo4j.server;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.logging.log4j.LogConfig.createLoggerFromXmlConfig;
import static org.neo4j.server.HeapDumpDiagnostics.INSTANCE;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.SystemUtils;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.BufferingLog;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.logging.log4j.SystemLogger;
import org.neo4j.memory.MachineMemory;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.startup.Environment;
import org.neo4j.server.startup.PidFileHelper;
import org.neo4j.util.FeatureToggles;
import org.neo4j.util.VisibleForTesting;
import sun.misc.Signal;

public abstract class NeoBootstrapper implements Bootstrapper {
    public static final String SIGTERM = "TERM";
    public static final String SIGINT = "INT";
    public static final int OK = 0;
    public static final int WEB_SERVER_STARTUP_ERROR_CODE = 1;
    public static final int GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;
    public static final int INVALID_CONFIGURATION_ERROR_CODE = 3;
    public static final int LICENSE_NOT_ACCEPTED_ERROR_CODE = 4;
    private static final String NEO4J_SLF4J_PROVIDER = "org.neo4j.server.logging.slf4j.SLF4JLogBridge";
    private static final boolean USE_NEO4J_SLF4J_PROVIDER =
            FeatureToggles.flag(Bootstrapper.class, "useNeo4jSlf4jProvider", false);

    @SuppressWarnings("unused")
    private static final HeapDumpDiagnostics HEAPDUMP_DIAGNOSTICS = INSTANCE; // Keep reference alive

    private volatile DatabaseManagementService databaseManagementService;
    private volatile Closeable userLogFileStream;
    private Thread shutdownHook;
    private GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
    private final BufferingLog startupLog = new BufferingLog();
    private volatile InternalLog log = startupLog;
    private String serverAddress = "unknown address";
    private String serverLocation = "unknown location";
    private MachineMemory machineMemory = MachineMemory.DEFAULT;
    private Path pidFile;

    public static int start(Bootstrapper boot, String... argv) {
        CommandLineArgs args = CommandLineArgs.parse(argv);

        if (args.homeDir == null) {
            throw new ServerStartupException("Argument --home-dir is required and was not provided.");
        }

        return boot.start(args.homeDir, args.configFile, args.configOverrides, args.expandCommands, !args.consoleMode);
    }

    @VisibleForTesting
    public final int start(Path homeDir, Map<String, String> configOverrides) {
        return start(homeDir, null, configOverrides, false, false);
    }

    /**
     *
     * @param homeDir NEO4J_HOME path.
     * @param configFile path to a possible configuration file, if the files does not exist a default config will be used.
     * @param configOverrides optional config overrides, will be applied after the {@code configFile} is parsed.
     * @param expandCommands if {@code true}, this will allow the config to execute commands in values and used the returned value instead.
     * @param daemonMode if {@code true}, console log appenders will be forcefully removed from the logging configuration.
     * @return exit code.
     */
    @Override
    public final int start(
            Path homeDir,
            Path configFile,
            Map<String, String> configOverrides,
            boolean expandCommands,
            boolean daemonMode) {
        addShutdownHook();
        installSignalHandlers();
        SystemLogger.installErrorListener();

        Config config = Config.newBuilder()
                .commandExpansion(expandCommands)
                .setDefaults(GraphDatabaseSettings.SERVER_DEFAULTS)
                .fromFileNoThrow(configFile)
                .setRaw(configOverrides)
                .set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath())
                .build();

        HeapDumpDiagnostics.INSTANCE.START_TIME = Instant.now().toString();
        HeapDumpDiagnostics.INSTANCE.NEO4J_VERSION = Version.getNeo4jVersion();

        pidFile = config.get(BootloaderSettings.pid_file);
        writePidSilently();

        Log4jLogProvider userLogProvider = setupLogging(config, daemonMode);
        userLogFileStream = userLogProvider;

        dependencies = dependencies.userLogProvider(userLogProvider);

        log = userLogProvider.getLog(getClass());

        boolean startAllowed = checkLicenseAgreement(homeDir, config, daemonMode);

        // Log any messages written before logging was configured.
        startupLog.replayInto(log);
        config.setLogger(log);

        if (SystemLogger.errorsEncounteredDuringSetup()) {
            // Refuse to start if there was a problem setting up the logging.
            return INVALID_CONFIGURATION_ERROR_CODE;
        }

        if (!startAllowed) {
            // Message should be printed by the checkLicenseAgreement call above
            return LICENSE_NOT_ACCEPTED_ERROR_CODE;
        }

        if (requestedMemoryExceedsAvailable(config)) {
            log.error(format(
                    "Invalid memory configuration - exceeds physical memory. Check the configured values for %s and %s",
                    GraphDatabaseSettings.pagecache_memory.name(), BootloaderSettings.max_heap_size.name()));
            return INVALID_CONFIGURATION_ERROR_CODE;
        }

        PrintStream daemonErr = daemonMode ? System.err : null;
        PrintStream daemonOut = daemonMode ? System.out : null;
        if (daemonMode) {
            // Redirect output to the log files
            SystemLogger.installStdRedirects(userLogProvider);
        }

        try (daemonOut;
                daemonErr) {
            serverAddress = config.get(HttpConnector.listen_address).toString();
            serverLocation = config.get(databases_root_path).toString();

            log.info("Starting...");
            databaseManagementService = createNeo(config, daemonMode, dependencies);
            if (daemonMode) {
                // Signal parent process we are ready to detach
                daemonErr.println(Environment.FULLY_FLEDGED);
            }
            log.info("Started.");
            return OK;
        } catch (ServerStartupException e) {
            e.describeTo(log);
            return WEB_SERVER_STARTUP_ERROR_CODE;
        } catch (TransactionFailureException tfe) {
            log.error(
                    format(
                            "Failed to start Neo4j on %s. Another process may be using databases at location: %s",
                            serverAddress, serverLocation),
                    tfe);
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        } catch (Exception e) {
            log.error(format("Failed to start Neo4j on %s.", serverAddress), e);
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    private void writePidSilently() {
        if (!SystemUtils.IS_OS_WINDOWS) // Windows does not use PID-files (for somewhat mysterious reasons)
        {
            try // The neo4j.pid should already be there, but in the case of using the `console --dry-run` functionality
            // we need to ensure it!
            {
                Long currentPid = ProcessHandle.current().pid();
                Long pid = PidFileHelper.readPid(pidFile);
                if (!currentPid.equals(pid)) {
                    PidFileHelper.storePid(pidFile, currentPid);
                }
            } catch (IOException ignored) {
            }
        }
    }

    private void deletePidSilently() {
        if (!SystemUtils.IS_OS_WINDOWS) {
            if (pidFile != null) {
                PidFileHelper.remove(pidFile);
            }
        }
    }

    private boolean requestedMemoryExceedsAvailable(Config config) {
        Long pageCacheMemory = config.get(GraphDatabaseSettings.pagecache_memory);
        long pageCacheSize = pageCacheMemory == null
                ? ConfiguringPageCacheFactory.defaultHeuristicPageCacheMemory(machineMemory)
                : pageCacheMemory;
        MemoryUsage heapMemoryUsage = machineMemory.getHeapMemoryUsage();
        long totalPhysicalMemory = machineMemory.getTotalPhysicalMemory();

        if (totalPhysicalMemory == 0) {
            log.warn(
                    "Unable to determine total physical memory of machine. JVM is most likely running in a container that do not expose that.");
            return false;
        }

        return totalPhysicalMemory != OsBeanUtil.VALUE_UNAVAILABLE
                && pageCacheSize + heapMemoryUsage.getMax() > totalPhysicalMemory;
    }

    @Override
    public int stop() {
        try {
            doShutdown();

            removeShutdownHook();

            closeUserLogFileStream();
            return 0;
        } catch (Exception e) {
            switchToErrorLoggingIfLoggingNotConfigured();
            log.error(
                    "Failed to cleanly shutdown Neo Server on port [%s], database [%s]. Reason [%s] ",
                    serverAddress, serverLocation, e.getMessage(), e);
            closeUserLogFileStream();
            return 1;
        }
    }

    public boolean isRunning() {
        return databaseManagementService != null;
    }

    public DatabaseManagementService getDatabaseManagementService() {
        return databaseManagementService;
    }

    public InternalLog getLog() {
        return log;
    }

    protected abstract DatabaseManagementService createNeo(
            Config config, boolean daemonMode, GraphDatabaseDependencies dependencies);

    protected abstract boolean checkLicenseAgreement(Path homeDir, Configuration config, boolean daemonMode);

    private static Log4jLogProvider setupLogging(Config config, boolean daemonMode) {
        Path xmlConfig = config.get(GraphDatabaseSettings.user_logging_config_path);
        boolean allowDefaultXmlConfig = !config.isExplicitlySet(GraphDatabaseSettings.user_logging_config_path);
        Neo4jLoggerContext ctx = createLoggerFromXmlConfig(
                new DefaultFileSystemAbstraction(),
                xmlConfig,
                allowDefaultXmlConfig,
                daemonMode,
                config::configStringLookup,
                null,
                null);

        ctx.getLogger(NeoBootstrapper.class).info("Logging config in use: " + ctx.getConfigSourceInfo());
        Log4jLogProvider userLogProvider = new Log4jLogProvider(ctx);

        JULBridge.resetJUL();
        Logger.getLogger("").setLevel(Level.WARNING);
        JULBridge.forwardTo(userLogProvider);
        setupSLF4JProvider(userLogProvider, List.of("org.eclipse.jetty"));
        return userLogProvider;
    }

    private static void setupSLF4JProvider(Log4jLogProvider userLogProvider, List<String> prefixFilters) {
        if (!USE_NEO4J_SLF4J_PROVIDER) {
            return;
        }

        try {
            // Load dynamically to allow user to remove the neo4j SLF4J provider and replace it another one
            Class<?> bridge = Class.forName(NEO4J_SLF4J_PROVIDER);
            Method setLogProvider = bridge.getMethod("setInstantiationContext", Log4jLogProvider.class, List.class);
            setLogProvider.invoke(null, userLogProvider, prefixFilters);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException
                | IllegalAccessException e) {
            userLogProvider
                    .getLog(NEO4J_SLF4J_PROVIDER)
                    .info(
                            "Neo4j SLF4J provider not found. Libraries that uses SLF4J, e.g. Jetty, will not be able to write the the Neo4j log files.");
            userLogProvider.getLog(NEO4J_SLF4J_PROVIDER).debug("Details: ", e);
        }
    }

    // Exit gracefully if possible
    private static void installSignalHandlers() {
        installSignalHandler(SIGTERM, false); // SIGTERM is invoked when system service is stopped
        installSignalHandler(SIGINT, true); // SIGINT is invoked when user hits ctrl-c  when running `neo4j console`
    }

    private static void installSignalHandler(String sig, boolean tolerateErrors) {
        try {
            // System.exit() will trigger the shutdown hook
            Signal.handle(new Signal(sig), signal -> System.exit(0));
        } catch (Throwable e) {
            if (!tolerateErrors) {
                throw e;
            }
            // Errors occur on IBM JDK with IllegalArgumentException: Signal already used by VM: INT
            // I can't find anywhere where we send a SIGINT to neo4j process so I don't think this is that important
        }
    }

    private void doShutdown() {
        switchToErrorLoggingIfLoggingNotConfigured();
        if (databaseManagementService != null) {
            log.info("Stopping...");
            databaseManagementService.shutdown();
        }
        deletePidSilently();
        log.info("Stopped.");
    }

    private void closeUserLogFileStream() {
        if (userLogFileStream != null) {
            closeAllUnchecked(userLogFileStream);
        }
    }

    private void addShutdownHook() {
        shutdownHook = new Thread(() -> {
            log.info("Neo4j Server shutdown initiated by request");
            doShutdown();
            closeUserLogFileStream();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void removeShutdownHook() {
        if (shutdownHook != null) {
            if (!Runtime.getRuntime().removeShutdownHook(shutdownHook)) {
                log.warn("Unable to remove shutdown hook");
            }
        }
    }

    /**
     * If we ran into an error before logging was properly setup we log what we have buffered and any following messages directly to System.out.
     */
    private void switchToErrorLoggingIfLoggingNotConfigured() {
        // Logging isn't configured yet
        if (userLogFileStream == null) {
            Log4jLogProvider outProvider = new Log4jLogProvider(System.out);
            userLogFileStream = outProvider;
            log = outProvider.getLog(getClass());
            startupLog.replayInto(log);
        }
    }

    @VisibleForTesting
    void setMachineMemory(MachineMemory machineMemory) {
        this.machineMemory = machineMemory;
    }
}
