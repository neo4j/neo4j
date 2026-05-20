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

import static java.lang.String.format;
import static java.nio.file.Files.readAllLines;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.logging.log4j.LogConfig.STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE;
import static org.neo4j.logging.log4j.LogConfig.createLoggerFromXmlConfig;
import static org.neo4j.logging.log4j.LogUtils.newLoggerBuilder;
import static org.neo4j.logging.log4j.LogUtils.newTemporaryXmlConfigBuilder;
import static org.neo4j.logging.log4j.LoggerTarget.ROOT_LOGGER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.logging.Level;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutput;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(SuppressOutputExtension.class)
@ResourceLock(Resources.SYSTEM_OUT)
class LogConfigTest {
    static final String DATE_PATTERN = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{4}";

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    @Inject
    private SuppressOutput suppressOutput;

    private Neo4jLoggerContext ctx;

    @AfterEach
    void tearDown() {
        ctx.close();
    }

    @Test
    void shouldRespectLogLevel() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilderToOutputStream(outContent, Level.DEBUG).build();

        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");
        logger.debug("test");
        logger.info("test");
        logger.warn("test");
        logger.error("test");

        String output = outContent.toString();
        assertThat(output)
                .contains(Level.DEBUG.toString())
                .contains(Level.INFO.toString())
                .contains(Level.WARN.toString())
                .contains(Level.ERROR.toString());

        outContent.reset();

        LogConfig.updateLogLevel(Level.WARN, ctx);

        logger.debug("test");
        logger.info("test");
        logger.warn("test");
        logger.error("test");

        output = outContent.toString();
        assertThat(output)
                .doesNotContain(Level.DEBUG.toString())
                .doesNotContain(Level.INFO.toString())
                .contains(Level.WARN.toString())
                .contains(Level.ERROR.toString());
    }

    @Test
    void withHeaderLoggerShouldBeUsedAsHeader() throws IOException {
        Path targetFile = dir.homePath().resolve("debug.log");
        Path targetFile1 = dir.homePath().resolve("debug.log.01");

        Path xmlConfig = newTemporaryXmlConfigBuilder(fs)
                .withLogger(newLoggerBuilder(ROOT_LOGGER, targetFile)
                        .withLevel(Level.INFO)
                        .withRotation(30, 2)
                        .withCategory(true)
                        .forDebugLog(true)
                        .build())
                .create();

        ctx = createLoggerFromXmlConfig(
                fs,
                xmlConfig,
                false,
                false,
                null,
                log -> {
                    log.warn("My Header");
                    log.warn("In Two Lines");
                },
                "org.neo4j.HeaderClassName");

        assertThat(targetFile).exists();

        ExtendedLogger logger = ctx.getLogger("className");

        logger.warn("Long line that will get next message to be written to next file");
        logger.warn("test2");

        assertThat(targetFile).exists();
        assertThat(targetFile1).exists();

        // First file (the one rotated to targetFile1) should not have the header.
        assertThat(targetFile1)
                .content(StandardCharsets.UTF_8)
                .matches(DATE_PATTERN
                        + format(
                                " %-5s \\[className] Long line that will get next message to be written to next file%n",
                                Level.WARN));

        assertThat(targetFile)
                .content(StandardCharsets.UTF_8)
                .matches(format(
                        DATE_PATTERN + " %-5s \\[o\\.n\\.HeaderClassName] My Header%n" + DATE_PATTERN
                                + " %-5s \\[o\\.n\\.HeaderClassName] In Two Lines%n" + DATE_PATTERN
                                + " %-5s \\[className] test2%n",
                        Level.WARN,
                        Level.WARN,
                        Level.WARN));
    }

    @Test
    void withOutputStreamShouldLogToTheStream() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        ctx = LogConfig.createBuilderToOutputStream(outContent, Level.INFO).build();

        ExtendedLogger logger = ctx.getLogger("test");
        logger.warn("test");

        assertThat(outContent.toString()).contains("test");
    }

    @Test
    void standardFormatDefaults() {
        Path targetFile = dir.homePath().resolve("debug.log");

        ctx = LogConfig.createTemporaryLoggerToSingleFile(fs, targetFile, Level.INFO, true);

        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");
        logger.warn("test");

        assertThat(targetFile)
                .content(StandardCharsets.UTF_8)
                .matches(DATE_PATTERN + format(" %-5s \\[o\\.n\\.classname] test%n", Level.WARN));
    }

    @Test
    void standardFormatNoCategory() {
        Path targetFile = dir.homePath().resolve("debug.log");

        ctx = LogConfig.createTemporaryLoggerToSingleFile(fs, targetFile, Level.INFO, false);

        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");
        logger.warn("test");

        assertThat(targetFile)
                .content(StandardCharsets.UTF_8)
                .matches(DATE_PATTERN + format(" %-5s test%n", Level.WARN));
    }

    @Test
    void jsonFormatDebugLog() {
        Path targetFile = dir.homePath().resolve("debug.log");

        Path xmlConfig = newTemporaryXmlConfigBuilder(fs)
                .withLogger(newLoggerBuilder(ROOT_LOGGER, targetFile)
                        .withLevel(Level.INFO)
                        .withCategory(true)
                        .withJsonFormatTemplate(STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE)
                        .build())
                .create();

        ctx = createLoggerFromXmlConfig(fs, xmlConfig);

        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");
        logger.warn("test");

        assertThat(targetFile)
                .content(StandardCharsets.UTF_8)
                .matches(format(
                        "\\{\"time\":\"" + DATE_PATTERN
                                + "\",\"level\":\"%s\",\"category\":\"o\\.n\\.classname\",\"message\":\"test\"}%n",
                        Level.WARN));
    }

    @Test
    void jsonFormatStacktrace() {
        Path targetFile = dir.homePath().resolve("debug.log");

        Path xmlConfig = newTemporaryXmlConfigBuilder(fs)
                .withLogger(newLoggerBuilder(ROOT_LOGGER, targetFile)
                        .withLevel(Level.INFO)
                        .withCategory(true)
                        .withJsonFormatTemplate(STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE)
                        .build())
                .create();

        ctx = createLoggerFromXmlConfig(fs, xmlConfig);

        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");
        logger.warn("test", newThrowable("stack"));

        assertThat(targetFile)
                .content(StandardCharsets.UTF_8)
                .matches(format(
                        "\\{\"time\":\"" + DATE_PATTERN
                                + "\",\"level\":\"%s\",\"category\":\"o\\.n\\.classname\",\"message\":\"test\",\"stacktrace\":\"stack\"}%n",
                        Level.WARN));
    }

    @Test
    void standardFormatWithStructuredMessage() {
        Path targetFile = dir.homePath().resolve("debug.log");

        ctx = LogConfig.createTemporaryLoggerToSingleFile(fs, targetFile, Level.INFO, true);

        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");
        logger.warn(new MyStructure());

        assertThat(targetFile)
                .content(StandardCharsets.UTF_8)
                .matches(DATE_PATTERN + format(" %-5s \\[o\\.n\\.classname] 1c%n", Level.WARN));
    }

    @Test
    void jsonFormatBigMessage() throws IOException {
        Path targetFile = dir.homePath().resolve("debug.log");

        int size = 1024 * 20;
        Path xmlConfig = newTemporaryXmlConfigBuilder(fs)
                .withLogger(newLoggerBuilder(ROOT_LOGGER, targetFile)
                        .withLevel(Level.INFO)
                        .withCategory(true)
                        .withJsonFormatTemplate(STRUCTURED_LOG_JSON_TEMPLATE_WITH_MESSAGE)
                        .withJsonStringMaxLength(32768)
                        .build())
                .create();

        ctx = createLoggerFromXmlConfig(fs, xmlConfig);
        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");

        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) 'x');

        logger.info(new String(bytes, StandardCharsets.UTF_8));
        String actual = Files.readString(targetFile);
        assertThat(actual.length()).isGreaterThan(size);
    }

    @Test
    void allowConsoleAppenders() throws IOException {
        useConsoleLogger(false);
    }

    @Test
    void disallowConsoleAppenders() throws IOException {
        useConsoleLogger(true);
    }

    private void useConsoleLogger(boolean daemonMode) throws IOException {
        String xml = """
                <Configuration>
                   <Appenders>
                       <File name="Neo4jLog" fileName="${config:server.directories.logs}/neo4j.log">
                           <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                       </File>
                       <Console name="ConsoleAppender" target="SYSTEM_OUT">
                           <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                       </Console>
                   </Appenders>

                   <Loggers>
                       <Root level="INFO">
                           <AppenderRef ref="Neo4jLog"/>
                           <AppenderRef ref="ConsoleAppender"/>
                       </Root>
                   </Loggers>
               </Configuration>
               """;
        Path xmlConfig = dir.homePath().resolve("user-logs.xml");
        FileSystemUtils.writeString(fs, xmlConfig, xml, EmptyMemoryTracker.INSTANCE);

        Map<String, Object> config =
                Map.of("server.directories.logs", dir.homePath().toAbsolutePath());
        ctx = createLoggerFromXmlConfig(fs, xmlConfig, false, daemonMode, config::get, null, null);

        ExtendedLogger logger = ctx.getLogger("org.neo4j.classname");
        logger.warn("test");

        Iterator<String> i = readAllLines(dir.homePath().resolve("neo4j.log")).iterator();
        if (daemonMode) {
            assertThat(i.next()).contains("Running in daemon mode, all <Console> appenders will be suppressed:");
            assertThat(i.next()).contains("Removing console appender 'ConsoleAppender' with target 'SYSTEM_OUT'.");
        }
        assertThat(i.next()).matches(DATE_PATTERN + format(" %-5s test", Level.WARN));
        assertThat(i).isExhausted();

        assertThat(suppressOutput.getOutputVoice().containsMessage(format(" %-5s test%n", Level.WARN)))
                .isNotEqualTo(daemonMode);
    }

    private static class MyStructure extends Neo4jMapMessage {
        MyStructure() {
            super(3);
            with("long", 7L);
            with("string1", "my string");
            with("string2", " special\" string");
        }

        @Override
        protected void formatAsString(StringBuilder stringBuilder) {
            stringBuilder.append(1).append("c");
        }
    }

    static Throwable newThrowable(final String stackTrace) {
        return new Throwable() {
            @Override
            public void printStackTrace(PrintWriter s) {
                s.append(stackTrace);
            }
        };
    }
}
