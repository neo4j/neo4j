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

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Optional;
import org.xml.sax.SAXParseException;

public class ConfigValidationIssue {
    private final Path file;
    private final String message;
    private final Throwable cause;
    private final boolean isError;

    public ConfigValidationIssue(Path file, String message, boolean isError, Throwable cause) {
        this.message = message;
        this.isError = isError;
        this.cause = cause;
        this.file = file;
    }

    private Optional<String> getLocation() {
        if (cause instanceof SAXParseException e) {
            return Optional.of("%d:%d".formatted(e.getLineNumber(), e.getColumnNumber()));
        }
        return Optional.empty();
    }

    public String getMessage() {
        String severity = isError ? "Error" : "Warning";
        String label = getLocation().map(loc -> " at " + loc).orElse("");
        return "%s%s: %s".formatted(severity, label, message);
    }

    public Throwable getThrowable() {
        return cause;
    }

    public ConfigValidationIssue asWarning() {
        if (isError) {
            return new ConfigValidationIssue(file, message, false, cause);
        }
        return this;
    }

    public void printStackTrace(PrintStream stream) {
        if (cause == null) {
            stream.println("No stack trace available.");
            return;
        }
        cause.printStackTrace(stream);
    }

    public boolean isError() {
        return isError;
    }
}
