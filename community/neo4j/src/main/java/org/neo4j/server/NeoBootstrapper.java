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
package org.neo4j.server;

import static java.lang.String.format;
import static org.neo4j.logging.log4j.LogConfig.createLoggerFromXmlConfig;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.nio.file.Path;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.SystemUtils;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.BufferingLog;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.memory.MachineMemory;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.logging.JettyLogBridge;
import org.neo4j.server.startup.PidFileHelper;
import org.neo4j.util.VisibleForTesting;
import sun.misc.Signal;

public abstract class NeoBootstrapper implements Bootstrapper {
    public static final String SIGTERM = "TERM";
    public static final String SIGINT = "INT";
    public static final int OK = 0;
    private static final int WEB_SERVER_STARTUP_ERROR_CODE = 1;
    private static final int GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;
    private static final int INVALID_CONFIGURATION_ERROR_CODE = 3;

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

        if (args.homeDir() == null) {
            throw new ServerStartupException("Argument --home-dir is required and was not provided.");
        }

        return boot.start(
                args.homeDir(),
                args.configFile(),
                args.configOverrides(),
                args.expandCommands(),
                args.allowConsoleAppenders());
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
     * @param allowConsoleAppenders if {@code false}, console log appenders will be forcefully removed from the logging configuration.
     * @return exit code.
     */
    @Override
    public final int start(
            Path homeDir,
            Path configFile,
            Map<String, String> configOverrides,
            boolean expandCommands,
            boolean allowConsoleAppenders) {
        addShutdownHook();
        installSignalHandlers();
        Config config = Config.newBuilder()
                .commandExpansion(expandCommands)
                .setDefaults(GraphDatabaseSettings.SERVER_DEFAULTS)
                .fromFileNoThrow(configFile)
                .setRaw(configOverrides)
                .set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath())
                .build();
        pidFile = config.get(BootloaderSettings.pid_file);
        writePidSilently();
        Log4jLogProvider userLogProvider = setupLogging(config, allowConsoleAppenders);
        userLogFileStream = userLogProvider;

        dependencies = dependencies.userLogProvider(userLogProvider);

        log = userLogProvider.getLog(getClass());

        checkLicenseAgreement(homeDir);

        // Log any messages written before logging was configured.
        startupLog.replayInto(log);

        config.setLogger(log);

        if (requestedMemoryExceedsAvailable(config)) {
            log.error(format(
                    "Invalid memory configuration - exceeds physical memory. Check the configured values for %s and %s",
                    GraphDatabaseSettings.pagecache_memory.name(), BootloaderSettings.max_heap_size.name()));
            return INVALID_CONFIGURATION_ERROR_CODE;
        }

        try {
            serverAddress = config.get(HttpConnector.listen_address).toString();
            serverLocation = config.get(GraphDatabaseInternalSettings.databases_root_path)
                    .toString();

            log.info("Starting...");
            databaseManagementService = createNeo(config, dependencies);
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

    protected abstract DatabaseManagementService createNeo(Config config, GraphDatabaseDependencies dependencies);

    protected abstract void checkLicenseAgreement(Path homeDir);

    private static Log4jLogProvider setupLogging(Config config, boolean allowConsoleAppenders) {
        Path xmlConfig = config.get(GraphDatabaseSettings.user_logging_config_path);
        boolean allowDefaultXmlConfig = !config.isExplicitlySet(GraphDatabaseSettings.user_logging_config_path);
        Neo4jLoggerContext ctx = createLoggerFromXmlConfig(
                new DefaultFileSystemAbstraction(),
                xmlConfig,
                allowDefaultXmlConfig,
                allowConsoleAppenders,
                config::configStringLookup,
                null,
                null);

        Log4jLogProvider userLogProvider = new Log4jLogProvider(ctx);

        JULBridge.resetJUL();
        Logger.getLogger("").setLevel(Level.WARNING);
        JULBridge.forwardTo(userLogProvider);
        JettyLogBridge.setLogProvider(userLogProvider);
        return userLogProvider;
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
            IOUtils.closeAllUnchecked(userLogFileStream);
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
