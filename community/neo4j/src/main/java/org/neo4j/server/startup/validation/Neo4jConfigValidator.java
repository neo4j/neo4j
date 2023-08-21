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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jLogMessage;
import org.neo4j.logging.Neo4jMessageSupplier;
import org.neo4j.util.VisibleForTesting;

public class Neo4jConfigValidator implements ConfigValidator {
    private final Path path;
    private final Supplier<Config> configSupplier;

    public Neo4jConfigValidator(Supplier<Config> config, Path path) {
        this.configSupplier = config;
        this.path = path;
    }

    @Override
    public List<ConfigValidationIssue> validate() {
        List<ConfigValidationIssue> issues = new ArrayList<>();
        try {
            // Calling the supplier should build the config,
            // and we'll record log messages and catch any exceptions here.
            var config = configSupplier.get();
            var logger = new IssueCollectingLogger(issues);

            // Will replay logging calls to our logger
            config.setLogger(logger);
        } catch (IllegalArgumentException e) {
            issues.add(new ConfigValidationIssue(path, e.getMessage(), true, e));
        } catch (CommandFailedException e) {
            // When validating using the bootloader, exceptions are wrapped in a CommandFailedException.
            var message = e.getMessage();
            var cause = e.getCause();
            if (cause != null) {
                message = cause.getMessage();
            }

            issues.add(new ConfigValidationIssue(path, message, true, e));
        }

        return issues;
    }

    @Override
    public String getLabel() {
        if (path != null) {
            return "Neo4j configuration: %s".formatted(path.toString());
        } else {
            return "Neo4j configuration";
        }
    }

    /**
     * Collects warnings and errors into list of issues
     */
    @VisibleForTesting
    static class IssueCollectingLogger implements InternalLog {
        private final List<ConfigValidationIssue> issues;

        public IssueCollectingLogger(List<ConfigValidationIssue> issues) {
            this.issues = issues;
        }

        @Override
        public void debug(Neo4jLogMessage message) {}

        @Override
        public void debug(Neo4jMessageSupplier supplier) {}

        @Override
        public void info(Neo4jLogMessage message) {}

        @Override
        public void info(Neo4jMessageSupplier supplier) {}

        @Override
        public void warn(Neo4jLogMessage message) {
            warn(message.getFormattedMessage(), message.getThrowable());
        }

        @Override
        public void warn(Neo4jMessageSupplier supplier) {
            warn(supplier.get());
        }

        @Override
        public void error(Neo4jLogMessage message) {
            error(message, message.getThrowable());
        }

        @Override
        public void error(Neo4jMessageSupplier supplier) {
            error(supplier.get());
        }

        @Override
        public void error(Neo4jLogMessage message, Throwable throwable) {
            error(message.getFormattedMessage(), throwable);
        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public void debug(String message) {}

        @Override
        public void debug(String message, Throwable throwable) {}

        @Override
        public void debug(String format, Object... arguments) {}

        @Override
        public void info(String message) {}

        @Override
        public void info(String message, Throwable throwable) {}

        @Override
        public void info(String format, Object... arguments) {}

        @Override
        public void warn(String message) {
            warn(Neo4jMessageSupplier.forMessage(message));
        }

        @Override
        public void warn(String message, Throwable throwable) {
            issues.add(new ConfigValidationIssue(null, message, false, throwable));
        }

        @Override
        public void warn(String format, Object... arguments) {
            warn(Neo4jMessageSupplier.forMessage(format, arguments));
        }

        @Override
        public void error(String message) {
            error(Neo4jMessageSupplier.forMessage(message));
        }

        @Override
        public void error(String message, Throwable throwable) {
            issues.add(new ConfigValidationIssue(null, message, true, throwable));
        }

        @Override
        public void error(String format, Object... arguments) {
            error(Neo4jMessageSupplier.forMessage(format, arguments));
        }
    }
}
