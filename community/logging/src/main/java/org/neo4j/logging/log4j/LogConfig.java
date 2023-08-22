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
package org.neo4j.logging.log4j;

import static org.neo4j.logging.log4j.LogUtils.newLoggerBuilder;
import static org.neo4j.logging.log4j.LogUtils.newTemporaryXmlConfigBuilder;
import static org.neo4j.logging.log4j.LoggerTarget.ROOT_LOGGER;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.layout.template.json.JsonTemplateLayout;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.status.StatusLogger;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.LogTimeZone;

public final class LogConfig {
    public static final String DEBUG_LOG = "debug.log";
    public static final String USER_LOG = "neo4j.log";
    public static final String QUERY_LOG = "query.log";
    public static final String SECURITY_LOG = "security.log";
    public static final String HTTP_LOG = "http.log";

    public static final String QUERY_LOG_JSON_TEMPLATE = "classpath:org/neo4j/logging/QueryLogJsonLayout.json";
    public static final String STRUCTURED_LOG_JSON_TEMPLATE = "classpath:org/neo4j/logging/StructuredJsonLayout.json";
    public static final String STRUCTURED_LOG_JSON_TEMPLATE_WITH_CATEGORY =
            "classpath:org/neo4j/logging/StructuredLayoutWithCategory.json";
    public static final String STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE =
            "classpath:org/neo4j/logging/StructuredLayoutWithMessage.json";

    public static final String SERVER_LOGS_XML = "server-logs.xml";
    public static final String USER_LOGS_XML = "user-logs.xml";
    private static final Map<Path, String> KNOWN_DEFAULTS = Map.of(
            Path.of(SERVER_LOGS_XML), "default-server-logs.xml", //
            Path.of(USER_LOGS_XML), "default-user-logs.xml");

    private LogConfig() {}

    static void updateLogLevel(org.neo4j.logging.Level level, Neo4jLoggerContext context) {
        LoggerContext log4jContext = (LoggerContext) context.getLoggerContext();
        Configuration config = log4jContext.getConfiguration();

        LoggerConfig loggerConfig = config.getRootLogger();
        loggerConfig.setLevel(convertNeo4jLevelToLevel(level));

        // This causes all Loggers to refresh information from their LoggerConfig.
        log4jContext.updateLoggers();
    }

    /**
     * Create a logger context from a log4j xml configuration file.
     *
     * @param fs the file system.
     * @param xmlConfigFile the log4j xml configuration path.
     * @return a logger context configured with the provided xml file.
     */
    public static Neo4jLoggerContext createLoggerFromXmlConfig(FileSystemAbstraction fs, Path xmlConfigFile) {
        return createLoggerFromXmlConfig(fs, xmlConfigFile, false, null);
    }

    /**
     * Create a logger context from a log4j xml configuration file.
     *
     * @param fs            the file system.
     * @param xmlConfigFile the log4j xml configuration path.
     * @param configLookup  a lookup function to get values for setting names.
     * @return a logger context configured with the provided xml file.
     */
    public static Neo4jLoggerContext createLoggerFromXmlConfig(
            FileSystemAbstraction fs,
            Path xmlConfigFile,
            boolean useDefaultOnMissingXml,
            Function<String, Object> configLookup) {
        return createLoggerFromXmlConfig(fs, xmlConfigFile, useDefaultOnMissingXml, false, configLookup, null, null);
    }

    /**
     * Create a logger context from a log4j xml configuration file.
     *
     * @param fs              the file system.
     * @param xmlConfigFile   the log4j xml configuration path.
     * @param daemonMode      if {@code true}, console appenders will be removed.
     * @param configLookup    a lookup function to get values for setting names.
     * @param headerLogger    a callback for header the header logger, only applicable to {@link Neo4jDebugLogLayout}.
     * @param headerClassName classname to use in the header logger, only applicable to {@link Neo4jDebugLogLayout}.
     * @return a logger context configured with the provided xml file.
     */
    public static Neo4jLoggerContext createLoggerFromXmlConfig(
            FileSystemAbstraction fs,
            Path xmlConfigFile,
            boolean useDefaultOnMissingXml,
            boolean daemonMode,
            Function<String, Object> configLookup,
            Consumer<InternalLog> headerLogger,
            String headerClassName) {
        return new Builder(fs, xmlConfigFile)
                .withConfigLookup(configLookup)
                .withHeaderLogger(headerLogger, headerClassName)
                .withUseDefaultOnMissingXml(useDefaultOnMissingXml)
                .withDaemonMode(daemonMode)
                .build();
    }

    /**
     * Create a logger context that will output to the provided path. Useful when writing tests.
     *
     * @param fs           the file system.
     * @param logPath      the output log file.
     * @param level        the desired log level.
     * @param withCategory whether to include the classname or not.
     * @return a new logger context configured with the provided values.
     */
    public static Neo4jLoggerContext createTemporaryLoggerToSingleFile(
            FileSystemAbstraction fs, Path logPath, org.neo4j.logging.Level level, boolean withCategory) {
        Path xmlConfig = newTemporaryXmlConfigBuilder(fs)
                .withLogger(newLoggerBuilder(ROOT_LOGGER, logPath)
                        .withLevel(convertNeo4jLevelToLevel(level).toString())
                        .withCategory(withCategory)
                        .build())
                .create();
        return new Builder(fs, xmlConfig).build();
    }

    /**
     * Start construction of a {@link Neo4jLoggerContext} that will write to a {@link OutputStream}.
     *
     * @param outputStream where log messages will be serialized to.
     * @param level the desired log level.
     * @return
     */
    public static Builder createBuilderToOutputStream(OutputStream outputStream, org.neo4j.logging.Level level) {
        return new Builder(outputStream, level);
    }

    /**
     * Handle injection of variables during xml configuration parsing, e.g. {@code ${config:dbms.directories.neo4j_home}}.
     */
    private static class LookupInjectionXmlConfiguration extends XmlConfiguration {
        private final LookupContext context;
        private final boolean daemonMode;
        private final List<String> removedAppenders = new ArrayList<>();

        LookupInjectionXmlConfiguration(
                LoggerContext loggerContext,
                ConfigurationSource configSource,
                LookupContext context,
                boolean daemonMode) {
            super(loggerContext, configSource);
            this.context = context;
            this.daemonMode = daemonMode;
        }

        @Override
        protected void doConfigure() {
            AbstractLookup.setLookupContext(context);
            super.doConfigure();
            AbstractLookup.removeLookupContext();

            if (daemonMode) {
                List<ConsoleAppender> consoleAppenders = getAppenders().values().stream()
                        .filter(ConsoleAppender.class::isInstance)
                        .map(ConsoleAppender.class::cast)
                        .toList();
                for (ConsoleAppender consoleAppender : consoleAppenders) {
                    removedAppenders.add("Removing console appender '" + consoleAppender.getName() + "' with target '"
                            + consoleAppender.getTarget() + "'.");
                    removeAppender(consoleAppender.getName());
                }
            }

            // Inject header logger to the debug log pattern
            for (Appender appender : getAppenders().values()) {
                Layout<?> layout = appender.getLayout();
                if (layout instanceof Neo4jDebugLogLayout neo4jDebugLogLayout) {
                    neo4jDebugLogLayout.setHeaderLogger(context.headerLogger(), context.headerClassName());
                }
            }
        }

        @Override
        public Configuration reconfigure() {
            try {
                final ConfigurationSource source = getConfigurationSource().resetInputStream();
                if (source == null) {
                    return null;
                }
                return new LookupInjectionXmlConfiguration(getLoggerContext(), source, context, daemonMode);
            } catch (final IOException ex) {
                StatusLogger.getLogger().error("Cannot locate file {}", getConfigurationSource(), ex);
            }
            return null;
        }
    }

    static Level convertNeo4jLevelToLevel(org.neo4j.logging.Level level) {
        return switch (level) {
            case ERROR -> Level.ERROR;
            case WARN -> Level.WARN;
            case INFO -> Level.INFO;
            case DEBUG -> Level.DEBUG;
            case NONE -> Level.OFF;
        };
    }

    public static class Builder {
        private final FileSystemAbstraction fileSystemAbstraction;
        private final Path externalConfigPath;
        private final Level level;
        private final OutputStream outputStream;
        private boolean includeCategory = true;
        private Consumer<InternalLog> headerLogger;
        private String headerClassName;
        private Function<String, Object> configLookup;
        private String jsonLayout;
        private boolean useDefaultOnMissingXml = false;
        private boolean daemonMode = false;
        private String configSourceInfo = "<programmatically>";

        private Builder(FileSystemAbstraction fileSystemAbstraction, Path xmlConfigFile) {
            this.fileSystemAbstraction = fileSystemAbstraction;
            this.externalConfigPath = xmlConfigFile;
            this.outputStream = null;
            this.level = null;
        }

        private Builder(OutputStream outputStream, org.neo4j.logging.Level level) {
            this.outputStream = outputStream;
            this.level = convertNeo4jLevelToLevel(level);
            this.fileSystemAbstraction = null;
            this.externalConfigPath = null;
        }

        public Builder withConfigLookup(Function<String, Object> configLookup) {
            this.configLookup = configLookup;
            return this;
        }

        public Builder withCategory(boolean includeCategory) {
            this.includeCategory = includeCategory;
            return this;
        }

        public Builder withHeaderLogger(Consumer<InternalLog> headerLogger, String headerClassName) {
            this.headerLogger = headerLogger;
            this.headerClassName = headerClassName;
            return this;
        }

        public Builder withJsonLayout(String jsonLayout) {
            this.jsonLayout = jsonLayout;
            return this;
        }

        public Builder withUseDefaultOnMissingXml(boolean useDefaultOnMissingXml) {
            this.useDefaultOnMissingXml = useDefaultOnMissingXml;
            return this;
        }

        public Builder withDaemonMode(boolean daemonMode) {
            this.daemonMode = daemonMode;
            return this;
        }

        public Neo4jLoggerContext build() {
            LoggerContext context = new LoggerContext("LoggerContext");
            if (outputStream != null) {
                configureLoggingForStream(context, this);
            } else {
                try {
                    ConfigurationSource configurationSource = getConfigurationSource();
                    configureLoggingFromFile(
                            context, headerLogger, headerClassName, configLookup, configurationSource, daemonMode);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return new Neo4jLoggerContext(context, null, configSourceInfo);
        }

        private ConfigurationSource getConfigurationSource() throws IOException {
            ConfigurationSource configurationSource = null;
            if (fileSystemAbstraction.fileExists(externalConfigPath)) {
                if (fileSystemAbstraction.isPersistent()) {
                    configurationSource = ConfigurationSource.fromUri(externalConfigPath.toUri());
                    configSourceInfo = "File '%s'".formatted(externalConfigPath.toAbsolutePath());
                } else {
                    // non-persistent file system, we need to use a stream here

                    // NOTE: For now, log4j will write to the real file system, since we have no way to inject
                    // our own file system. This can be solved by porting our file system abstraction to the
                    // Java file system. And use e.g. Path.of("tmpfs://logs/debug.log")

                    configurationSource =
                            new ConfigurationSource(fileSystemAbstraction.openAsInputStream(externalConfigPath));
                }
            } else {
                if (useDefaultOnMissingXml) {
                    // On missing a xml file, we will try to use a default one for known files
                    String defaultResourcePath = KNOWN_DEFAULTS.get(externalConfigPath.getFileName());
                    if (defaultResourcePath != null) {
                        configurationSource = ConfigurationSource.fromResource(
                                defaultResourcePath, getClass().getClassLoader());
                        configSourceInfo = "Embedded default config '%s'".formatted(defaultResourcePath);
                    }
                }
            }
            if (configurationSource == null) {
                throw new IllegalStateException("Missing xml file for " + externalConfigPath);
            }
            return configurationSource;
        }
    }

    public static String getFormatPattern(boolean includeCategory, LogTimeZone timezone) {
        String date = "%d{yyyy-MM-dd HH:mm:ss.SSSZ}" + (timezone == LogTimeZone.UTC ? "{GMT+0}" : "");
        return includeCategory ? date + " %-5p [%c{1.}] %m%n" : date + " %-5p %m%n";
    }

    private static void configureLoggingForStream(LoggerContext context, Builder builder) {
        Configuration configuration = new Neo4jConfiguration();

        Appender appender = OutputStreamAppender.newBuilder()
                .setName("neo4jLog.stream")
                .setTarget(builder.outputStream)
                .setLayout(
                        builder.jsonLayout == null
                                ? PatternLayout.newBuilder()
                                        .withPattern(getFormatPattern(builder.includeCategory, LogTimeZone.UTC))
                                        .build()
                                : JsonTemplateLayout.newBuilder()
                                        .setConfiguration(configuration)
                                        .setEventTemplateUri(builder.jsonLayout)
                                        .build())
                .build();
        appender.start();
        configuration.addAppender(appender);
        configuration.getRootLogger().addAppender(appender, null, null);
        configuration.getRootLogger().setLevel(builder.level);
        context.setConfiguration(configuration);
    }

    private static void configureLoggingFromFile(
            LoggerContext context,
            Consumer<InternalLog> headerLogger,
            String headerClassName,
            Function<String, Object> configLookup,
            ConfigurationSource configSource,
            boolean daemonMode) {
        LookupContext lookupContext = new LookupContext(headerLogger, headerClassName, configLookup);
        LookupInjectionXmlConfiguration lookupInjectionXmlConfiguration =
                new LookupInjectionXmlConfiguration(context, configSource, lookupContext, daemonMode);
        context.setConfiguration(lookupInjectionXmlConfiguration);
        if (daemonMode && !lookupInjectionXmlConfiguration.removedAppenders.isEmpty()) {
            ExtendedLogger logger = context.getLogger(LogConfig.class);
            logger.info("Running in daemon mode, all <Console> appenders will be suppressed:");
            lookupInjectionXmlConfiguration.removedAppenders.forEach(logger::info);
        }
    }
}
