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
package org.neo4j.server.startup;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.Predicates.notNull;
import static org.neo4j.server.startup.BootloaderOsAbstraction.UNKNOWN_PID;
import static org.neo4j.server.startup.validation.ConfigValidationSummary.ValidationResult.ERRORS;
import static org.neo4j.server.startup.validation.ConfigValidationSummary.ValidationResult.OK;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.factory.Lists;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExitCode;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.IOUtils;
import org.neo4j.server.startup.validation.ConfigValidationHelper;
import org.neo4j.time.Stopwatch;
import org.neo4j.util.VisibleForTesting;

/**
 * Bootloader is used for launching either a DBMS ({@link Bootloader.Dbms}) or a forked admin command ({@link Bootloader.Admin}).
 */
public abstract class Bootloader implements AutoCloseable {

    static final int EXIT_CODE_OK = ExitCode.OK;
    static final int EXIT_CODE_RUNNING = ExitCode.FAIL;
    static final int EXIT_CODE_NOT_RUNNING = 3;

    static final String ENV_NEO4J_HOME = "NEO4J_HOME";
    static final String ENV_NEO4J_CONF = "NEO4J_CONF";
    static final String ENV_NEO4J_SHUTDOWN_TIMEOUT = "NEO4J_SHUTDOWN_TIMEOUT";
    static final String ENV_HEAP_SIZE = "HEAP_SIZE";
    static final String ENV_JAVA_OPTS = "JAVA_OPTS";
    static final String PROP_JAVA_CP = "java.class.path";
    static final String PROP_VM_NAME = "java.vm.name";
    static final String PROP_BASEDIR = "basedir";

    static final String ARG_EXPAND_COMMANDS = "--expand-commands";
    static final String ARG_CONSOLE_MODE = "--console-mode";

    static final Path DEFAULT_CONFIG_LOCATION = Path.of(Config.DEFAULT_CONFIG_DIR_NAME);
    static final int DEFAULT_NEO4J_SHUTDOWN_TIMEOUT = 120;

    // With console mode we can allow console appenders

    final Class<?> entrypoint;
    final Environment environment;

    // init
    final boolean verbose;
    final boolean expandCommands;
    final List<String> additionalArgs;

    // inferred
    private Path home;
    private Path conf;
    private FilteredConfig config;
    private boolean fullConfig;
    private BootloaderOsAbstraction os;
    private ProcessManager processManager;
    private URLClassLoader pluginClassloader;

    protected Bootloader(
            Class<?> entrypoint,
            Environment environment,
            boolean expandCommands,
            boolean verbose,
            String... additionalArgs) {
        this.entrypoint = entrypoint;
        this.environment = environment;

        this.expandCommands = expandCommands;
        this.verbose = verbose;
        this.additionalArgs = Lists.mutable.with(additionalArgs);
    }

    String getEnv(String key) {
        return getEnv(key, "", SettingValueParsers.STRING);
    }

    <T> T getEnv(String key, T defaultValue, SettingValueParser<T> parser) {
        return getValue(key, defaultValue, parser, environment.envLookup());
    }

    String getProp(String key) {
        return getProp(key, "", SettingValueParsers.STRING);
    }

    <T> T getProp(String key, T defaultValue, SettingValueParser<T> parser) {
        return getValue(key, defaultValue, parser, environment.propLookup());
    }

    private <T> T getValue(String key, T defaultValue, SettingValueParser<T> parser, Function<String, String> lookup) {
        String value = lookup.apply(key);
        try {
            return StringUtils.isNotEmpty(value) ? parser.parse(value) : defaultValue;
        } catch (IllegalArgumentException e) {
            throw new CommandFailedException("Failed to parse value for " + key + ". " + e.getMessage(), e, 1);
        }
    }

    Path home() {
        if (home == null) {
            Path defaultHome = getProp(
                    PROP_BASEDIR,
                    Path.of("").toAbsolutePath().getParent(),
                    PATH); // Basedir is provided by the app-assembler
            home = getEnv(ENV_NEO4J_HOME, defaultHome, PATH).toAbsolutePath(); // But a NEO4J_HOME has higher prio
        }
        return home;
    }

    public Path confDir() {
        if (conf == null) {
            conf = getEnv(ENV_NEO4J_CONF, home().resolve(DEFAULT_CONFIG_LOCATION), PATH);
        }
        return conf;
    }

    public Path confFile() {
        return confDir().resolve(Config.DEFAULT_CONFIG_FILE_NAME);
    }

    public FilteredConfig fullConfig() {
        return config(true, false);
    }

    protected void validateConfigVerbose(boolean silentOnSuccess) {
        var helper = new ConfigValidationHelper(confFile());
        var summary = helper.validateAll(() -> fullConfig().getUnfiltered());

        if (silentOnSuccess) {
            // Don't print anything at all if we only have warnings.
            if (summary.result() == ERRORS) {
                summary.print(environment.err(), verbose);
                summary.printClosingStatement(environment.err());
            }
        } else {
            // Don't print anything if all is well
            if (summary.result() != OK) {
                summary.print(environment.err(), verbose);
                summary.printClosingStatement(environment.out());
            }
        }

        if (summary.result() == ERRORS) {
            throw new CommandFailedException(
                    "Configuration contains errors. This validation can be performed again using '"
                            + ValidateConfigCommand.COMMAND + "'.",
                    ExitCode.FAIL);
        }
    }

    protected void validateConfig() {
        config(true, false);
    }

    void rebuildConfig(List<Path> additionalConfigs) {
        this.config = buildConfig(true, additionalConfigs, confFile(), false);
        this.fullConfig = true;
    }

    public FilteredConfig config() {
        return config(false, false);
    }

    private FilteredConfig config(boolean full, boolean allowThrow) {
        if (config == null || !fullConfig && full) {
            this.config = buildConfig(full, List.of(), confFile(), allowThrow);
            this.fullConfig = full;
        }
        return config;
    }

    private FilteredConfig buildConfig(
            boolean full, List<Path> additionalConfigs, Path mainConfFile, boolean allowThrow) {
        try {
            Predicate<String> filter = full ? alwaysTrue() : settingsUsedByBootloader()::contains;

            var builder = getConfigBuilder(full)
                    .commandExpansion(expandCommands)
                    .setDefaults(overriddenDefaultsValues())
                    .set(GraphDatabaseSettings.neo4j_home, home())
                    .fromFile(mainConfFile, allowThrow, filter);

            Collections.reverse(additionalConfigs);
            additionalConfigs.forEach(additionalConfig -> builder.fromFile(additionalConfig, false, filter));

            return new FilteredConfig(builder.build(), filter);
        } catch (RuntimeException e) {
            if (additionalConfigs.isEmpty()) {
                throw new CommandFailedException("Failed to read config " + mainConfFile + ": " + e.getMessage(), e);
            } else {
                throw new CommandFailedException("Failed to read config: " + e.getMessage(), e);
            }
        }
    }

    private Config.Builder getConfigBuilder(boolean loadPluginsSettings) {
        if (loadPluginsSettings) {
            var classloader = getPluginClassLoader();
            if (classloader != null) {
                return Config.newBuilder(classloader);
            }
        }
        return Config.newBuilder();
    }

    ClassLoader getPluginClassLoader() {
        if (pluginClassloader == null) {
            // Locate plugin jar files and add them to the config class loader
            try (Stream<Path> list = Files.list(config().get(GraphDatabaseSettings.plugin_dir))) {
                URL[] urls = list.filter(path -> path.toString().endsWith(".jar"))
                        .map(this::pathToURL)
                        .filter(notNull())
                        .toArray(URL[]::new);

                if (urls.length > 0) {
                    pluginClassloader = new URLClassLoader(urls, Bootloader.class.getClassLoader());
                }
            } catch (IOException e) {
                if (verbose) {
                    e.printStackTrace(environment.err());
                }
            }
        }
        return pluginClassloader;
    }

    private URL pathToURL(Path p) {
        try {
            return p.toUri().toURL();
        } catch (MalformedURLException e) {
            if (verbose) {
                e.printStackTrace(environment.err());
            }
            return null;
        }
    }

    private static Set<String> settingsUsedByBootloader() {
        // These settings are the that might be used by the bootloader minor commands (stop/status etc..)
        // Additional settings are used on the start/console path, but they use the full config anyway so not added
        // here.
        return Set.of(
                GraphDatabaseSettings.neo4j_home.name(),
                GraphDatabaseSettings.logs_directory.name(),
                GraphDatabaseSettings.plugin_dir.name(),
                GraphDatabaseSettings.strict_config_validation.name(),
                GraphDatabaseInternalSettings.config_command_evaluation_timeout.name(),
                BootloaderSettings.run_directory.name(),
                BootloaderSettings.additional_jvm.name(),
                BootloaderSettings.lib_directory.name(),
                BootloaderSettings.windows_service_name.name(),
                BootloaderSettings.windows_tools_directory.name(),
                BootloaderSettings.pid_file.name());
    }

    protected abstract Map<Setting<?>, Object> overriddenDefaultsValues();

    BootloaderOsAbstraction os() {
        if (os == null) {
            os = BootloaderOsAbstraction.getOsAbstraction(this);
        }
        return os;
    }

    ProcessManager processManager() {
        if (processManager == null) {
            processManager = new ProcessManager(this);
        }
        return processManager;
    }

    Runtime.Version version() {
        return environment.version();
    }

    private static String pidIfKnown(long pid) {
        return pid != UNKNOWN_PID ? " (pid:" + pid + ")" : "";
    }

    protected void printDirectories() {
        Configuration config = config();

        var out = environment.out();
        out.println("Directories in use:");
        out.println("home:         " + home().toAbsolutePath());
        out.println("config:       " + confDir().toAbsolutePath());
        out.println("logs:         "
                + config.get(GraphDatabaseSettings.logs_directory).toAbsolutePath());
        out.println(
                "plugins:      " + config.get(GraphDatabaseSettings.plugin_dir).toAbsolutePath());
        out.println("import:       "
                + config.get(GraphDatabaseSettings.load_csv_file_url_root).toAbsolutePath());
        out.println("data:         "
                + config.get(GraphDatabaseSettings.data_directory).toAbsolutePath());
        out.println("certificates: "
                + home().resolve("certificates").toAbsolutePath()); // this is no longer an individual setting
        out.println("licenses:     "
                + config.get(GraphDatabaseSettings.licenses_directory).toAbsolutePath());
        out.println(
                "run:          " + config.get(BootloaderSettings.run_directory).toAbsolutePath());
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(pluginClassloader);
    }

    public static class FilteredConfig implements Configuration {
        private final Config config;
        private final Predicate<String> filter;

        public FilteredConfig(Config config, Predicate<String> filter) {
            this.config = config;
            this.filter = filter;
        }

        public Object configStringLookup(String name) {
            throwIfNotInFilter(name);
            return config.configStringLookup(name);
        }

        public Config getUnfiltered() {
            return config;
        }

        @Override
        public <T> T get(Setting<T> setting) {
            throwIfNotInFilter(setting.name());
            return config.get(setting);
        }

        private void throwIfNotInFilter(String name) {
            if (!filter.test(name)) {
                // This is to prevent silent error and should only be encountered while developing. Just add the
                // setting to the filter!
                throw new IllegalArgumentException(
                        "Not allowed to read this setting " + name + ". It has been filtered out");
            }
        }
    }

    public static class Dbms extends Bootloader {
        public Dbms(Environment environment, boolean expandCommands, boolean verbose) {
            this(EntryPoint.serviceloadEntryPoint(), environment, expandCommands, verbose);
        }

        @VisibleForTesting
        Dbms(Class<?> entrypoint, Environment environment, boolean expandCommands, boolean verbose) {
            super(entrypoint, environment, expandCommands, verbose);
            if (expandCommands) {
                this.additionalArgs.add(ARG_EXPAND_COMMANDS);
            }
        }

        @Override
        protected Map<Setting<?>, Object> overriddenDefaultsValues() {
            return GraphDatabaseSettings.SERVER_DEFAULTS;
        }

        void start() {
            BootloaderOsAbstraction os = os();
            validateConfigVerbose(false);

            Optional<Long> runningProcess = os.getPidIfRunning();
            if (runningProcess.isPresent()) {
                throw new CommandFailedException(
                        String.format("Neo4j is already running%s.", pidIfKnown(runningProcess.get())),
                        EXIT_CODE_RUNNING);
            }

            printDirectories();
            environment.out().println("Starting Neo4j.");
            long pid;
            try {
                pid = os.start();
            } catch (CommandFailedException e) {
                environment.err().println(e.getMessage());
                throw new CommandFailedException("Unable to start. See user log for details.", e, e.getExitCode());
            }

            String serverLocation;
            Configuration config = config();
            if (config.get(HttpsConnector.enabled)) {
                serverLocation = "It is available at https://" + config.get(HttpsConnector.listen_address);
            } else if (config.get(HttpConnector.enabled)) {
                serverLocation = "It is available at http://" + config.get(HttpConnector.listen_address);
            } else {
                serverLocation = "Both http & https are disabled.";
            }
            environment.out().printf("Started neo4j%s. %s%n", pidIfKnown(pid), serverLocation);
            environment.out().println("There may be a short delay until the server is ready.");
        }

        void console(boolean dryRun) {
            BootloaderOsAbstraction os = os();
            validateConfigVerbose(dryRun);

            Optional<Long> runningProcess = os.getPidIfRunning();

            this.additionalArgs.add(ARG_CONSOLE_MODE);

            if (dryRun) {
                List<String> args = os.buildStandardStartArguments();
                String cmd = args.stream().map(Dbms::quoteArgument).collect(Collectors.joining(" "));
                environment.out().println(cmd);

                if (!runningProcess.isPresent()) {
                    // If not already running, we are done.
                    // If already running, we will error out in the next step.
                    return;
                }
            }

            if (runningProcess.isPresent()) {
                throw new CommandFailedException(
                        String.format("Neo4j is already running%s.", pidIfKnown(runningProcess.get())),
                        EXIT_CODE_RUNNING);
            }

            printDirectories();
            environment.out().println("Starting Neo4j.");
            os.console();
        }

        void stop(Integer maybeTimeout) {
            BootloaderOsAbstraction os = os();
            Optional<Long> runningProcess = os.getPidIfRunning();
            if (runningProcess.isEmpty()) {
                environment.out().println("Neo4j is not running.");
                return;
            }
            environment.out().print("Stopping Neo4j.");
            int timeout;
            if (maybeTimeout != null) {
                timeout = maybeTimeout;
            } else {
                timeout = getEnv(ENV_NEO4J_SHUTDOWN_TIMEOUT, DEFAULT_NEO4J_SHUTDOWN_TIMEOUT, INT);
            }

            Stopwatch stopwatch = Stopwatch.start();
            long pid = runningProcess.get();
            os.stop(pid);
            int printCount = 0;
            do {
                if (!os.isRunning(pid)) {
                    environment.out().println(" stopped.");
                    return;
                }

                if (stopwatch.hasTimedOut(printCount, SECONDS)) {
                    printCount++;
                    environment.out().print(".");
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } while (!stopwatch.hasTimedOut(timeout, SECONDS));

            environment.out().println(" failed to stop.");
            environment
                    .out()
                    .printf(
                            "Neo4j%s took more than %d seconds to stop.%n",
                            pidIfKnown(pid), stopwatch.elapsed(SECONDS));
            environment.out().printf("Please see logs/neo4j.log for details.%n");
            throw new CommandFailedException("Failed to stop", EXIT_CODE_RUNNING);
        }

        void restart(Integer maybeTimeout) {
            stop(maybeTimeout);
            start();
        }

        void status() {
            Optional<Long> runningProcess = os().getPidIfRunning();
            if (runningProcess.isEmpty()) {
                throw new CommandFailedException("Neo4j is not running.", EXIT_CODE_NOT_RUNNING);
            }
            long pid = runningProcess.get();
            environment.out().printf("Neo4j is running%s%n", pid != UNKNOWN_PID ? " at pid " + pid : "");
        }

        /**
         * @throws CommandFailedException when something goes wrong. This exception is automatically handled by
         *                                {@link org.neo4j.cli.AbstractCommand}
         */
        void installService() {
            validateConfigVerbose(false);
            if (os().serviceInstalled()) {
                throw new CommandFailedException("Neo4j service is already installed.", EXIT_CODE_RUNNING);
            }
            os().installService();
            environment.out().println("Neo4j service installed.");
        }

        /**
         * @throws CommandFailedException when something goes wrong. This exception is automatically handled by
         *                                {@link org.neo4j.cli.AbstractCommand}
         */
        void uninstallService() {
            if (!os().serviceInstalled()) {
                environment.out().println("Neo4j service is not installed");
                return;
            }
            os().uninstallService();
            environment.out().println("Neo4j service uninstalled.");
        }

        void updateService() {
            validateConfigVerbose(false);
            if (!os().serviceInstalled()) {
                throw new CommandFailedException("Neo4j service is not installed", EXIT_CODE_NOT_RUNNING);
            }
            os().updateService();
            environment.out().println("Neo4j service updated.");
        }

        /**
         *  This is written with inspiration from apache commons-exec `StringUtils.quoteArgument()`.
         *  That implementation contains a bug (fixed here) that could trim away quotes erroneously in the start or end of string
         *  E.g turn "a partly 'quoted string'" to " a partly 'quoted string" missing the last single quote.
         */
        private static String quoteArgument(String arg) {

            final String singleQuote = "'";
            final String doubleQuote = "\"";
            arg = arg.trim();
            while (arg.length() > 2
                    && (arg.startsWith(singleQuote) && arg.endsWith(singleQuote)
                            || arg.startsWith(doubleQuote) && arg.endsWith(doubleQuote))) {
                arg = arg.substring(1, arg.length() - 1);
            }

            if (arg.contains(doubleQuote)) {
                if (arg.contains(singleQuote)) {
                    throw new CommandFailedException("`" + arg
                            + "` contains both single and double quotes. Can not be correctly quoted for commandline.");
                }
                arg = singleQuote + arg + singleQuote;
            } else if (arg.contains(singleQuote) || arg.contains(" ")) {
                arg = doubleQuote + arg + doubleQuote;
            }
            return arg;
        }
    }

    public static class Admin extends Bootloader {

        public Admin(
                Class<?> entrypoint,
                Environment environment,
                boolean expandCommands,
                boolean verbose,
                String... additionalArgs) {
            super(entrypoint, environment, expandCommands, verbose, additionalArgs);
        }

        @Override
        protected Map<Setting<?>, Object> overriddenDefaultsValues() {
            return Map.of();
        }

        int admin(List<Path> additionalConfigs) {
            try {
                if (!additionalConfigs.isEmpty()) {
                    rebuildConfig(additionalConfigs);
                }
                validateConfig();
                os().admin();
                return EXIT_CODE_OK;
            } catch (BootProcessFailureException e) {
                return e.getExitCode(); // NOTE! This is not the generic BootFailureException, it indicates a process
                // non-zero exit, not bootloader failure.
            }
        }
    }
}
