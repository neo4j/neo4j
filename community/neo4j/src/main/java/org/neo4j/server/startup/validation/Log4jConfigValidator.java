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
package org.neo4j.server.startup.validation;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Reconfigurable;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.util.NullOutputStream;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.logging.log4j.AbstractLookup;
import org.neo4j.logging.log4j.LookupContext;
import org.neo4j.util.VisibleForTesting;
import org.xml.sax.SAXParseException;

public class Log4jConfigValidator implements ConfigValidator {
    private final Path path;
    private final Config config;
    private final String label;
    static final String[] NONSENSE_ERRORS = {"No logging configuration"};

    public Log4jConfigValidator(Supplier<Config> config, String label, Path path) {
        this.config = config.get();
        this.path = path;
        this.label = label;
    }

    @Override
    public List<ConfigValidationIssue> validate() throws IOException {
        // XmlConfiguration doesn't throw an exception when it encounters an
        // error - it logs it both to stderr and to StatusLogger.getLogger().
        // So let's temporarily silence stderr and stdout, then read the config,
        // while listening for log messages on the status logger.
        StatusLogger logger = StatusLogger.getLogger();
        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;

        List<ConfigValidationIssue> issues = new ArrayList<>();
        var statusListener = createIssueCollectingStatusListener(issues);

        try {
            System.setOut(new PrintStream(NullOutputStream.nullOutputStream()));
            System.setErr(new PrintStream(NullOutputStream.nullOutputStream()));
            logger.registerListener(statusListener);
            AbstractLookup.setLookupContext(new LookupContext(null, null, config::configStringLookup));
            loadConfig(path);
        } finally {
            AbstractLookup.removeLookupContext();
            logger.clear();
            logger.removeListener(statusListener);
            System.setOut(oldOut);
            System.setErr(oldErr);
        }

        return issues;
    }

    private StatusListener createIssueCollectingStatusListener(List<ConfigValidationIssue> issues) {
        return new StatusListener() {
            @Override
            public void close() {}

            @Override
            public void log(StatusData status) {
                String message = status.getMessage().getFormattedMessage();
                Throwable throwable = status.getThrowable();

                if (throwable instanceof SAXParseException) {
                    message = throwable.getMessage();
                }

                if (!ArrayUtil.contains(NONSENSE_ERRORS, message)) {
                    issues.add(new ConfigValidationIssue(path, message, true, throwable));
                }
            }

            @Override
            public Level getStatusLevel() {
                return Level.ERROR;
            }
        };
    }

    @VisibleForTesting
    void loadConfig(Path path) {
        // If not found, error is logged to StatusLogger
        var source = ConfigurationSource.fromUri(path.toUri());
        if (source != null) {
            var config = new XmlConfigValidator(source);
            config.initialize();

            // We need to stop() the config to remove its appenders, and
            // to do that it first needs to be started.
            config.start();
            config.stop();
        }
    }

    private static class XmlConfigValidator extends XmlConfiguration {
        public XmlConfigValidator(ConfigurationSource source) {
            super(null, source);
        }

        @Override
        protected void initializeWatchers(
                Reconfigurable reconfigurable, ConfigurationSource configSource, int monitorIntervalSeconds) {
            // Don't
        }
    }

    @Override
    public String getLabel() {
        return "%s Log4j configuration: %s".formatted(label, path.toString());
    }
}
