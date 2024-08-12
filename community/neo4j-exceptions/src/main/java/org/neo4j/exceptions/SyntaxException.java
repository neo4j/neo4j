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
package org.neo4j.exceptions;

import static java.lang.System.lineSeparator;

import java.util.Optional;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SyntaxException extends Neo4jException {
    private final transient Optional<Integer> offset;
    private final String query;

    public SyntaxException(String message, String query, Optional<Integer> offset, Throwable cause) {
        super(message, cause);
        this.offset = offset;
        this.query = query;
    }

    public SyntaxException(
            ErrorGqlStatusObject gqlStatusObject,
            String message,
            String query,
            Optional<Integer> offset,
            Throwable cause) {
        super(gqlStatusObject, message, cause);

        this.offset = offset;
        this.query = query;
    }

    public SyntaxException(String message, String query, int offset) {
        this(message, query, Optional.of(offset), null);
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message, String query, int offset) {
        this(gqlStatusObject, message, query, Optional.of(offset), null);
    }

    public SyntaxException(String message, String query, int offset, Throwable cause) {
        this(message, query, Optional.of(offset), cause);
    }

    public SyntaxException(
            ErrorGqlStatusObject gqlStatusObject, String message, String query, int offset, Throwable cause) {
        this(gqlStatusObject, message, query, Optional.of(offset), cause);
    }

    public SyntaxException(String message, Throwable cause) {
        this(message, "", Optional.empty(), cause);
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        this(gqlStatusObject, message, "", Optional.empty(), cause);
    }

    public SyntaxException(String message) {
        this(message, "", Optional.empty(), null);
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message) {
        this(gqlStatusObject, message, "", Optional.empty(), null);
    }

    @Override
    public Status status() {
        return Status.Statement.SyntaxError;
    }

    public Optional<Integer> getOffset() {
        return offset;
    }

    @Override
    public String getMessage() {
        if (offset.isPresent()) {
            // split can be empty if query = '\n'
            var split = query.split("\n");
            return super.getMessage()
                    + lineSeparator()
                    + findErrorLine(offset.get(), split.length != 0 ? split : new String[] {""});
        } else {
            return super.getMessage();
        }
    }

    private static String findErrorLine(int offset, String[] message) {
        int currentOffset = offset;
        if (message.length == 0) {
            throw new IllegalArgumentException("message converted to empty list");
        } else {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < message.length; i++) {
                String element = message[i];
                if (i < message.length - 1) {
                    if (element.length() >= currentOffset) {
                        buildErrorString(builder, element, currentOffset);
                        break;
                    } else {
                        // The extra minus one is there for the now missing \n
                        currentOffset -= element.length() + 1;
                    }
                } else {
                    buildErrorString(builder, element, Math.min(element.length(), currentOffset));
                }
            }
            return builder.toString();
        }
    }

    private static void buildErrorString(StringBuilder builder, String element, int currentOffset) {
        builder.append("\"")
                .append(element.stripTrailing()) // removes potential \r at the end
                .append("\"")
                .append(lineSeparator())
                .append(" ".repeat(currentOffset + 1)) // extra space to compensate for an opening quote
                .append('^');
    }
}
