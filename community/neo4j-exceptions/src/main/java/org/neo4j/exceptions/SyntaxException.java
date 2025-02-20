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
import static java.util.Objects.nonNull;

import java.util.Optional;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class SyntaxException extends Neo4jException {

    private final transient Integer offset;
    private final String query;

    @Deprecated
    public SyntaxException(String message, String query, Integer offset, Throwable cause) {
        super(message, cause);
        this.offset = offset;
        this.query = query;
    }

    public SyntaxException(
            ErrorGqlStatusObject gqlStatusObject, String message, String query, Integer offset, Throwable cause) {
        super(gqlStatusObject, message, cause);

        this.offset = offset;
        this.query = query;
    }

    @Deprecated
    public SyntaxException(String message, String query, int offset) {
        this(message, query, offset, null);
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message, String query, int offset) {
        this(gqlStatusObject, message, query, offset, null);
    }

    @Deprecated
    public SyntaxException(String message, Throwable cause) {
        this(message, "", null, cause);
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        this(gqlStatusObject, message, "", null, cause);
    }

    @Deprecated
    public SyntaxException(String message) {
        this(message, "", null, null);
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message) {
        this(gqlStatusObject, message, "", null, null);
    }

    public static SyntaxException invalidShortestPathException(String start) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N33)
                        .build())
                .build();
        return new SyntaxException(
                gql,
                String.format(
                        "To find a shortest path, both ends of the path need to be provided. Couldn't find `%s`",
                        start));
    }

    public static SyntaxException wrongNumberOfArguments(
            int expectedCount, int actualCount, String name, String signature) {
        var msg = String.format(
                "The procedure or function call does not provide the required number of arguments; expected %s but got %s. "
                        + "The procedure or function `%s` has the signature: `%s`.",
                expectedCount, actualCount, name, signature);
        return wrongNumberOfArguments(expectedCount, actualCount, name, signature, msg);
    }

    public static SyntaxException wrongNumberOfArguments(
            int expectedCount, int actualCount, String name, String signature, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I13)
                        .withParam(GqlParams.NumberParam.count1, expectedCount)
                        .withParam(GqlParams.NumberParam.count2, actualCount)
                        .withParam(GqlParams.StringParam.procFun, name)
                        .withParam(GqlParams.StringParam.sig, signature)
                        .build())
                .build();
        return new SyntaxException(gql, legacyMessage);
    }

    public static SyntaxException variableAlreadyBound(String variable, String clause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N78)
                .withParam(GqlParams.StringParam.variable, variable)
                .withParam(GqlParams.StringParam.clause, clause)
                .build();
        return new SyntaxException(
                gql,
                String.format(
                        "Can't create node `%s` with labels or properties here. The variable is already declared in this context",
                        variable));
    }

    public static SyntaxException accessingMultipleGraphsOnlySupportedOnCompositeDatabases(
            String legacyMessage, String query, int offset) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA5)
                        .build())
                .build();
        return new SyntaxException(gql, legacyMessage, query, offset);
    }

    public static SyntaxException invalidNestedUseClause(
            String db1, String db2, String legacyMessage, String query, int offset) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N74)
                        .withParam(GqlParams.StringParam.db1, db1)
                        .withParam(GqlParams.StringParam.db2, db2)
                        .build())
                .build();
        return new SyntaxException(gql, legacyMessage, query, offset);
    }

    @Override
    public Status status() {
        return Status.Statement.SyntaxError;
    }

    public Optional<Integer> getOffset() {
        return Optional.ofNullable(offset);
    }

    @Override
    public String getMessage() {
        if (nonNull(offset)) {
            // split can be empty if query = '\n'
            var split = query.split("\n");
            return super.getMessage()
                    + lineSeparator()
                    + findErrorLine(offset, split.length != 0 ? split : new String[] {""});
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
