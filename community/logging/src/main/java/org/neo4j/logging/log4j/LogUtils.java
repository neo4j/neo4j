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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.neo4j.logging.log4j.LogConfig.getFormatPattern;
import static org.neo4j.logging.log4j.LoggerTarget.ROOT_LOGGER;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.memory.EmptyMemoryTracker;

public final class LogUtils {

    private LogUtils() {}

    public static Log4jXmlConfigBuilder newTemporaryXmlConfigBuilder(FileSystemAbstraction fileSystem) {
        try {
            return new Log4jXmlConfigBuilder(fileSystem, fileSystem.createTempFile("log4j-", ".xml"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Log4jXmlConfigBuilder newXmlConfigBuilder(
            FileSystemAbstraction fileSystemAbstraction, Path xmlConfig) {
        return new Log4jXmlConfigBuilder(fileSystemAbstraction, xmlConfig);
    }

    public static Log4jXmlLoggerBuilder newLoggerBuilder(LoggerTarget target, Path filename) {
        return new Log4jXmlLoggerBuilder(target, filename);
    }

    public static class Log4jXmlLoggerBuilder {
        private final LoggerTarget target;
        private final Path filename;
        private String level = "info";
        private boolean createOnDemand;
        private LogTimeZone timezone = LogTimeZone.UTC;
        private boolean includeCategory = true;
        private String jsonLayout;
        private long rotationThreshold = ByteUnit.mebiBytes(20);
        private int maxArchives = 7;
        private boolean forDebugLog = false;

        Log4jXmlLoggerBuilder(LoggerTarget target, Path filename) {
            this.target = target;
            this.filename = filename;
        }

        public Log4jXmlLoggerBuilder withLevel(String level) {
            this.level = level;
            return this;
        }

        public Log4jXmlLoggerBuilder withLevel(Level level) {
            this.level = LogConfig.convertNeo4jLevelToLevel(level).toString();
            return this;
        }

        public Log4jXmlLoggerBuilder withRotation(long rotationThreshold, int maxArchives) {
            this.rotationThreshold = rotationThreshold;
            this.maxArchives = maxArchives;
            return this;
        }

        public Log4jXmlLoggerBuilder createOnDemand() {
            this.createOnDemand = true;
            return this;
        }

        public Log4jXmlLoggerBuilder withCategory(boolean includeCategory) {
            this.includeCategory = includeCategory;
            return this;
        }

        public Log4jXmlLoggerBuilder withJsonFormatTemplate(String templateUri) {
            this.jsonLayout = templateUri;
            return this;
        }

        public Log4jXmlLoggerBuilder forDebugLog(boolean forDebugLog) {
            this.forDebugLog = forDebugLog;
            return this;
        }

        public Log4jXmlLoggerBuilder withTimezone(LogTimeZone timezone) {
            this.timezone = timezone;
            return this;
        }

        public XmlLogger build() {
            return new XmlLogger(
                    target,
                    filename,
                    level,
                    createOnDemand,
                    timezone,
                    includeCategory,
                    jsonLayout,
                    rotationThreshold,
                    maxArchives,
                    forDebugLog);
        }
    }

    record XmlLogger(
            LoggerTarget target,
            Path filename,
            String level,
            boolean createOnDemand,
            LogTimeZone timezone,
            boolean includeCategory,
            String jsonLayout,
            long rotationThreshold,
            int maxArchives,
            boolean forDebugLog) {
        private String getPattern() {
            return getFormatPattern(includeCategory, timezone);
        }
    }

    public static final class Log4jXmlConfigBuilder {
        private final FileSystemAbstraction fileSystem;
        private final Path xmlConfig;

        private final List<XmlLogger> loggers = new ArrayList<>();

        private Log4jXmlConfigBuilder(FileSystemAbstraction fileSystem, Path xmlConfig) {
            this.fileSystem = fileSystem;
            this.xmlConfig = xmlConfig;
        }

        public Log4jXmlConfigBuilder withLogger(XmlLogger logger) {
            loggers.add(logger);
            return this;
        }

        public Path create() {
            StringBuilder sb = new StringBuilder();
            sb.append("<Configuration>\n");
            sb.append("  <Appenders>\n");
            createAppender(sb);
            sb.append("  </Appenders>\n");
            sb.append("    <Loggers>\n");
            for (XmlLogger logger : loggers) {
                if (logger.target.equals(ROOT_LOGGER)) {
                    sb.append("      <Root level=\"").append(logger.level).append("\">\n");
                    sb.append("        <AppenderRef ref=\"appender-")
                            .append(logger.target.getTarget())
                            .append("\"/>\n");
                    sb.append("      </Root>\n");
                } else {
                    sb.append("      <Logger name=\"")
                            .append(logger.target.getTarget())
                            .append("\" additivity=\"false\" level=\"")
                            .append(logger.level)
                            .append("\">\n");
                    sb.append("        <AppenderRef ref=\"appender-")
                            .append(logger.target.getTarget())
                            .append("\"/>\n");
                    sb.append("      </Logger>\n");
                }
            }
            sb.append("    </Loggers>\n");
            sb.append("</Configuration>\n");

            byte[] xmlContent = sb.toString().getBytes(StandardCharsets.UTF_8);

            try {
                fileSystem.mkdirs(xmlConfig.getParent());
                try (StoreChannel channel = fileSystem.open(xmlConfig, Set.of(CREATE, WRITE, TRUNCATE_EXISTING));
                        var scopedBuffer = new NativeScopedBuffer(
                                xmlContent.length, ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE)) {
                    ByteBuffer buffer = scopedBuffer.getBuffer();
                    buffer.put(xmlContent);
                    buffer.flip();
                    channel.writeAll(buffer);
                }
                return xmlConfig;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void createAppender(StringBuilder sb) {
            for (XmlLogger logger : loggers) {
                String appender = "RollingRandomAccessFile";
                if (logger.createOnDemand) {
                    // RollingRandomAccessFile does not support createOnDemand
                    appender = "RollingFile";
                }

                sb.append("    <")
                        .append(appender)
                        .append(" name=\"appender-")
                        .append(logger.target.getTarget())
                        .append("\" fileName=\"")
                        .append(logger.filename.toAbsolutePath())
                        .append("\" filePattern=\"")
                        .append(logger.filename.toAbsolutePath())
                        .append(".%02i\"");

                if (logger.createOnDemand) {
                    sb.append(" createOnDemand=\"true\"");
                }
                sb.append(">\n");

                if (logger.jsonLayout != null) {
                    sb.append("      <JsonTemplateLayout eventTemplateUri=\"")
                            .append(logger.jsonLayout)
                            .append("\" />\n");
                } else if (logger.forDebugLog) {
                    sb.append("      <Neo4jDebugLogLayout pattern=\"")
                            .append(logger.getPattern())
                            .append("\"/>\n");
                } else {
                    sb.append("      <PatternLayout pattern=\"")
                            .append(logger.getPattern())
                            .append("\"/>\n");
                }
                sb.append("      <Policies>\n");
                sb.append("        <SizeBasedTriggeringPolicy size=\"")
                        .append(logger.rotationThreshold)
                        .append(" B\"/>\n");
                sb.append("      </Policies>\n");
                sb.append("      <DefaultRolloverStrategy fileIndex=\"min\" max=\"")
                        .append(logger.maxArchives)
                        .append("\"/>\n");

                sb.append("    </").append(appender).append(">\n");
            }
        }
    }
}
