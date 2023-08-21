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
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.logging.log4j.LogConfig.getFormatPattern;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.Level;
import org.neo4j.logging.log4j.LogConfig;

/**
 * As of Neo4j 5.0 logging settings have moved to log4j 2 configurations files.
 * Here we try to generate the xml equivalent to the settings from a Neo4j 4.x configuration file.
 */
class LoggingSettingsMigrator {
    private static final String DEFAULT_PLAIN_LAYOUT = "PatternLayout";

    private final Map<String, String> rawConfig;
    private final PrintStream out;
    private final Path destinationConfigFile;

    LoggingSettingsMigrator(PrintStream out, Path destinationConfigFile, Map<String, String> rawConfig) {
        this.out = out;
        this.destinationConfigFile = destinationConfigFile;

        this.rawConfig = rawConfig;

        // Manually migrate dependencies
        if (this.rawConfig.containsKey("dbms.directories.neo4j_home")) {
            this.rawConfig.put(neo4j_home.name(), this.rawConfig.get("dbms.directories.neo4j_home"));
        }
        if (this.rawConfig.containsKey("dbms.directories.logs")) {
            this.rawConfig.put(logs_directory.name(), this.rawConfig.get("dbms.directories.logs"));
        }
    }

    public void migrate() throws IOException {
        Path userLogsXlm = destinationConfigFile.resolveSibling(LogConfig.USER_LOGS_XML);
        preserveOriginal(userLogsXlm);
        String migratedUserLogs = migratedUserLogs();
        Files.writeString(userLogsXlm, migratedUserLogs);
        out.println("User logging configuration xml file generated: " + userLogsXlm);

        Path serverLogsXlm = destinationConfigFile.resolveSibling(LogConfig.SERVER_LOGS_XML);
        preserveOriginal(serverLogsXlm);
        String migratedServerLogs = migratedServerLogs();
        Files.writeString(serverLogsXlm, migratedServerLogs);
        out.println("Server logging configuration xml file generated: " + serverLogsXlm);
    }

    private void preserveOriginal(Path configFile) throws IOException {
        if (Files.exists(configFile)) {
            Path preservedFilePath = configFile.getParent().resolve(configFile.getFileName() + ".old");
            out.println("Keeping original " + configFile.getFileName() + " file at: " + preservedFilePath);
            Files.move(configFile, preservedFilePath);
        }
    }

    private String migratedUserLogs() {
        StringBuilder sb = new StringBuilder();

        appendHeader(sb);

        appendAppender(
                sb,
                "Neo4jLog",
                getRelativeFileName(OldSettings.store_user_log_path),
                getSettingValue(OldSettings.store_user_log_rotation_threshold),
                getSettingValue(OldSettings.store_user_log_max_archives),
                DEFAULT_PLAIN_LAYOUT,
                getSettingValue(OldSettings.store_user_log_format),
                LogConfig.STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE,
                false);

        sb.append(
                """

                        <!-- Only used by "neo4j console", will be ignored otherwise -->
                """);
        sb.append(format("        <Console name=\"ConsoleAppender\" target=\"SYSTEM_OUT\">%n"));
        addLayout(
                sb,
                getSettingValue(OldSettings.store_user_log_format),
                DEFAULT_PLAIN_LAYOUT,
                LogConfig.STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE,
                false);
        sb.append(
                """
                        </Console>
                    </Appenders>

                    <Loggers>
                        <!-- Log level for the neo4j log. One of DEBUG, INFO, WARN, ERROR or OFF -->
                        <Root level="INFO">
                            <AppenderRef ref="Neo4jLog"/>
                            <AppenderRef ref="ConsoleAppender"/>
                        </Root>
                    </Loggers>
                </Configuration>

                """);

        return sb.toString();
    }

    private String migratedServerLogs() {
        StringBuilder sb = new StringBuilder();
        appendHeader(sb);

        appendAppender(
                sb,
                "DebugLog",
                getRelativeFileName(OldSettings.store_internal_log_path),
                getSettingValue(OldSettings.store_internal_log_rotation_threshold),
                getSettingValue(OldSettings.store_internal_log_max_archives),
                "Neo4jDebugLogLayout",
                getSettingValue(OldSettings.store_internal_log_format),
                LogConfig.STRUCTURED_LOG_JSON_TEMPLATE_WITH_CATEGORY,
                true);

        appendAppender(
                sb,
                "HttpLog",
                getRelativeFileName(OldSettings.http_log_path),
                getSettingValue(OldSettings.http_logging_rotation_size),
                getSettingValue(OldSettings.http_logging_rotation_keep_number),
                DEFAULT_PLAIN_LAYOUT,
                getSettingValue(OldSettings.http_log_format),
                LogConfig.STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE,
                false);

        appendAppender(
                sb,
                "QueryLog",
                getRelativeFileName(OldSettings.log_queries_filename),
                getSettingValue(OldSettings.log_queries_rotation_threshold),
                getSettingValue(OldSettings.log_queries_max_archives),
                DEFAULT_PLAIN_LAYOUT,
                getSettingValue(OldSettings.log_query_format),
                LogConfig.QUERY_LOG_JSON_TEMPLATE,
                false);

        appendAppender(
                sb,
                "SecurityLog",
                getRelativeFileName(OldSettings.security_log_filename),
                getSettingValue(OldSettings.store_security_log_rotation_threshold),
                getSettingValue(OldSettings.store_security_log_max_archives),
                DEFAULT_PLAIN_LAYOUT,
                getSettingValue(OldSettings.security_log_format),
                LogConfig.STRUCTURED_LOG_JSON_TEMPLATE,
                false);

        sb.append(
                """
                    </Appenders>

                    <Loggers>
                        <!-- Log levels. One of DEBUG, INFO, WARN, ERROR or OFF -->

                        <!-- The debug log is used as the root logger to catch everything -->
                """);
        sb.append(format("        <Root level=\"%s\">%n", getSettingValue(OldSettings.store_internal_log_level)));
        sb.append(
                """
                            <AppenderRef ref="DebugLog"/> <!-- Keep this -->
                        </Root>

                        <!-- The query log, must be named "QueryLogger" -->
                        <Logger name="QueryLogger" level="INFO" additivity="false">
                            <AppenderRef ref="QueryLog"/>
                        </Logger>

                        <!-- The http request log, must be named "HttpLogger" -->
                        <Logger name="HttpLogger" level="INFO" additivity="false">
                            <AppenderRef ref="HttpLog"/>
                        </Logger>

                        <!-- The security log, must be named "SecurityLogger" -->
                """);
        sb.append(format(
                "        <Logger name=\"SecurityLogger\" level=\"%s\" additivity=\"false\">%n",
                getSettingValue(OldSettings.security_log_level)));
        sb.append(
                """
                            <AppenderRef ref="SecurityLog"/>
                        </Logger>
                    </Loggers>
                </Configuration>

                """);

        return sb.toString();
    }

    private void appendAppender(
            StringBuilder sb,
            String appenderName,
            String fileName,
            long rotationSize,
            int numFiles,
            String plainLayout,
            FormattedLogFormat logFormat,
            String jsonTemplate,
            boolean includeCategory) {
        if (rotationSize == 0) {
            sb.append(format("        <RandomAccessFile name=\"%s\" fileName=\"%s\">%n", appenderName, fileName));
            addLayout(sb, logFormat, plainLayout, jsonTemplate, includeCategory);
            sb.append(format("        </RandomAccessFile>%n"));
        } else {
            sb.append(format("        <RollingRandomAccessFile name=\"%s\" fileName=\"%s\"%n", appenderName, fileName));
            sb.append(format("                filePattern=\"%s.%%02i\">%n", fileName));
            sb.append(format("            <Policies>%n"));
            sb.append(format(
                    "                <SizeBasedTriggeringPolicy size=\"%s\"/>%n",
                    byteCountToDisplaySize(rotationSize)));
            sb.append(format("            </Policies>%n"));
            sb.append(format("            <DefaultRolloverStrategy fileIndex=\"min\" max=\"%d\"/>%n", numFiles));
            addLayout(sb, logFormat, plainLayout, jsonTemplate, includeCategory);
            sb.append(format("        </RollingRandomAccessFile>%n"));
        }
    }

    private void addLayout(
            StringBuilder sb,
            FormattedLogFormat logFormat,
            String plainLayout,
            String jsonTemplate,
            boolean includeCategory) {
        if (logFormat == FormattedLogFormat.PLAIN) {
            sb.append("            <")
                    .append(plainLayout)
                    .append(" pattern=\"")
                    .append(getFormatPattern(includeCategory, getSettingValue(db_timezone)))
                    .append(format("\"/>%n"));
        } else {
            sb.append(format("            <JsonTemplateLayout eventTemplateUri=\"%s\"/>%n", jsonTemplate));
        }
    }

    private void appendHeader(StringBuilder sb) {
        sb.append(
                """
                <?xml version="1.0" encoding="UTF-8"?>
                <!--
                    This is a log4j 2 configuration file that provides maximum flexibility.

                    All configuration values can be queried with the lookup prefix "config:". You can for example, resolve
                    the path to your neo4j home directory with ${config:dbms.directories.neo4j_home}.

                    Please consult https://logging.apache.org/log4j/2.x/manual/configuration.html for instructions and
                    available configuration options.
                -->
                <Configuration status="ERROR" monitorInterval="30" packages="org.neo4j.logging.log4j">
                    <Appenders>
                """);
    }

    private String getRelativeFileName(Setting<Path> pathSetting) {
        Path logDirectory = getSettingValue(logs_directory);
        return "${config:server.directories.logs}/"
                + logDirectory
                        .relativize(getSettingValue(pathSetting))
                        .toString()
                        .replace('\\', '/');
    }

    private <T> T getSettingValue(Setting<T> setting) {
        SettingImpl<T> impl = (SettingImpl<T>) setting;
        String value = rawConfig.get(impl.name());
        T v = value != null ? impl.parse(value) : impl.defaultValue();

        SettingImpl<T> dependency = impl.dependency();
        if (dependency != null) {
            return impl.parser().solveDependency(v, getSettingValue(dependency));
        }
        return v;
    }

    /**
     * Old 4.4 settings
     */
    private static final class OldSettings {
        private static final Setting<FormattedLogFormat> default_log_format = newBuilder(
                        "dbms.logs.default_format", ofEnum(FormattedLogFormat.class), FormattedLogFormat.PLAIN)
                .immutable()
                .build();

        // User log
        static final Setting<FormattedLogFormat> store_user_log_format = newBuilder(
                        "dbms.logs.user.format", ofEnum(FormattedLogFormat.class), null)
                .setDependency(default_log_format)
                .build();
        static final Setting<Path> store_user_log_path = newBuilder("dbms.logs.user.path", PATH, Path.of("neo4j.log"))
                .setDependency(logs_directory)
                .immutable()
                .build();
        static final Setting<Integer> store_user_log_max_archives = newBuilder(
                        "dbms.logs.user.rotation.keep_number", INT, 7)
                .addConstraint(min(1))
                .build();
        static final Setting<Long> store_user_log_rotation_threshold = newBuilder(
                        "dbms.logs.user.rotation.size", BYTES, 0L)
                .addConstraint(range(0L, Long.MAX_VALUE))
                .build();

        // Debug log
        static final Setting<FormattedLogFormat> store_internal_log_format = newBuilder(
                        "dbms.logs.debug.format", ofEnum(FormattedLogFormat.class), null)
                .setDependency(default_log_format)
                .build();
        static final Setting<Level> store_internal_log_level = newBuilder(
                        "dbms.logs.debug.level", ofEnum(Level.class), Level.INFO)
                .dynamic()
                .build();
        static final Setting<Path> store_internal_log_path = newBuilder(
                        "dbms.logs.debug.path", PATH, Path.of("debug.log"))
                .setDependency(GraphDatabaseSettings.logs_directory)
                .immutable()
                .build();
        static final Setting<Integer> store_internal_log_max_archives = newBuilder(
                        "dbms.logs.debug.rotation.keep_number", INT, 7)
                .addConstraint(min(1))
                .build();
        static final Setting<Long> store_internal_log_rotation_threshold = newBuilder(
                        "dbms.logs.debug.rotation.size", BYTES, mebiBytes(20))
                .addConstraint(range(0L, Long.MAX_VALUE))
                .build();

        // Http log
        static final Setting<FormattedLogFormat> http_log_format = newBuilder(
                        "dbms.logs.http.format", ofEnum(FormattedLogFormat.class), null)
                .setDependency(default_log_format)
                .build();
        static final Setting<Path> http_log_path = newBuilder("dbms.logs.http.path", PATH, Path.of("http.log"))
                .setDependency(logs_directory)
                .immutable()
                .build();
        static final Setting<Integer> http_logging_rotation_keep_number =
                newBuilder("dbms.logs.http.rotation.keep_number", INT, 5).build();
        static final Setting<Long> http_logging_rotation_size = newBuilder(
                        "dbms.logs.http.rotation.size", BYTES, ByteUnit.mebiBytes(20))
                .addConstraint(range(0L, Long.MAX_VALUE))
                .build();

        // Query log
        static final Setting<FormattedLogFormat> log_query_format = newBuilder(
                        "dbms.logs.query.format", ofEnum(FormattedLogFormat.class), null)
                .setDependency(default_log_format)
                .build();
        static final Setting<Path> log_queries_filename = newBuilder("dbms.logs.query.path", PATH, Path.of("query.log"))
                .setDependency(logs_directory)
                .immutable()
                .build();
        static final Setting<Integer> log_queries_max_archives = newBuilder(
                        "dbms.logs.query.rotation.keep_number", INT, 7)
                .addConstraint(min(1))
                .dynamic()
                .build();
        static final Setting<Long> log_queries_rotation_threshold = newBuilder(
                        "dbms.logs.query.rotation.size", BYTES, mebiBytes(20))
                .addConstraint(range(0L, Long.MAX_VALUE))
                .dynamic()
                .build();

        // Security
        static final Setting<FormattedLogFormat> security_log_format = newBuilder(
                        "dbms.logs.security.format", ofEnum(FormattedLogFormat.class), null)
                .setDependency(default_log_format)
                .build();
        static final Setting<Level> security_log_level = newBuilder(
                        "dbms.logs.security.level", ofEnum(Level.class), Level.INFO)
                .build();
        static final Setting<Path> security_log_filename = newBuilder(
                        "dbms.logs.security.path", PATH, Path.of("security.log"))
                .immutable()
                .setDependency(GraphDatabaseSettings.logs_directory)
                .build();
        static final Setting<Integer> store_security_log_max_archives = newBuilder(
                        "dbms.logs.security.rotation.keep_number", INT, 7)
                .addConstraint(min(1))
                .build();
        static final Setting<Long> store_security_log_rotation_threshold = newBuilder(
                        "dbms.logs.security.rotation.size", BYTES, ByteUnit.mebiBytes(20))
                .addConstraint(range(0L, Long.MAX_VALUE))
                .build();
    }
}
