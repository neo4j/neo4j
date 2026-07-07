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

import java.util.List;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class SyntaxException extends Neo4jException {

    private final transient Integer offset;
    private final String query;
    private String positionString = "";

    public static final String QUOTE_MISMATCH_ERROR_MESSAGE =
            "Failed to parse string literal. The query must contain an even number of non-escaped quotes.";

    protected SyntaxException(
            ErrorGqlStatusObject gqlStatusObject, String message, String query, Integer offset, Throwable cause) {
        super(gqlStatusObject, message, cause);

        this.offset = offset;
        this.query = query;
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message, String query, int offset) {
        this(gqlStatusObject, message, query, offset, null);
    }

    public SyntaxException(
            ErrorGqlStatusObject gqlStatusObject,
            String message,
            String query,
            String adjustedPosition,
            int offset,
            Throwable cause) {
        this(gqlStatusObject, message, query, offset, cause);
        if (nonNull(adjustedPosition)) {
            this.positionString = String.format(" (%s)", adjustedPosition);
        }
    }

    protected SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        this(gqlStatusObject, message, "", null, cause);
    }

    public SyntaxException(ErrorGqlStatusObject gqlStatusObject, String message) {
        this(gqlStatusObject, message, "", null, null);
    }

    public static SyntaxException internalError(String msgTitle, String message, String query, int offset) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new SyntaxException(gql, message, query, offset, null);
    }

    public static SyntaxException internalError(String msgTitle, String message) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new SyntaxException(gql, message);
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

    public static SyntaxException invalidInput(
            String input, List<String> expected, String legacyMessage, Integer offset) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I06)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.ListParam.valueList, expected)
                        .build())
                .build();
        return new SyntaxException(gql, legacyMessage, input, offset);
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

    public static SyntaxException stringLiteralWithInvalidQuotes() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I19)
                        .build())
                .build();
        return new SyntaxException(gql, QUOTE_MISMATCH_ERROR_MESSAGE);
    }

    public static SyntaxException cannotYieldFromVoidProcedure() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I42)
                        .build())
                .build();
        return new SyntaxException(gql, "Cannot yield value from void procedure.");
    }

    public static SyntaxException unknownFunction(String functionName, int offset, int line, int column) {
        var gql = GqlHelper.getGql42001_42N48(functionName, offset, line, column);
        return new SyntaxException(gql, String.format("Unknown function '%s'", functionName));
    }

    public static SyntaxException dynamicGraphReferenceUnsupported(
            String legacyMsg, String query, int offset, int line, int column) {
        var gql = GqlHelper.getGql42001_42N72(offset, line, column);
        return new SyntaxException(gql, legacyMsg, query, offset);
    }

    public static SyntaxException invalidUseOfAggregateFunction(String functionType, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N34)
                        .withParam(GqlParams.StringParam.funType, functionType)
                        .build())
                .build();
        return new SyntaxException(gql, legacyMessage);
    }

    public static SyntaxException invalidPartInPBAC(String expression, String invalidPart) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NAA)
                        .withParam(GqlParams.StringParam.expr, expression)
                        .withParam(GqlParams.StringParam.exprType, invalidPart)
                        .build())
                .build();
        return new SyntaxException(
                gql,
                String.format(
                        "The expression: `%s` is not supported. Lists containing %s values can not be used for property-based access control.",
                        expression, invalidPart));
    }

    public static SyntaxException nanInPBAC(String expression) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA3)
                        .build())
                .build();
        return new SyntaxException(
                gql,
                String.format(
                        "The expression: `%s` is not supported. `NaN` is not supported for property-based access control.",
                        expression));
    }

    public static SyntaxException nullInPBAC(String expression) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA4)
                        .withParam(GqlParams.StringParam.pred, expression)
                        .build())
                .build();
        return new SyntaxException(
                gql,
                String.format(
                        "The expression: `%s` is not supported. `NULL` is not supported for property-based access control.",
                        expression));
    }

    public static SyntaxException mixedListInPBAC(String expression) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NAB)
                        .withParam(GqlParams.StringParam.expr, expression)
                        .build())
                .build();
        return new SyntaxException(
                gql,
                String.format(
                        "The expression: `%s` is not supported. All elements in a list must be literals of the same type for property-based access control.",
                        expression));
    }

    @Override
    public Status status() {
        return Status.Statement.SyntaxError;
    }

    public OptionalInt getOffset() {
        return offset == null ? OptionalInt.empty() : OptionalInt.of(offset);
    }

    @Override
    public String getMessage() {
        return formatMessageWithPositionQueryAndOffset(super.getMessage());
    }

    @Override
    public String legacyMessage() {
        return formatMessageWithPositionQueryAndOffset(super.legacyMessage());
    }

    public String formatMessageWithPositionQueryAndOffset(String message) {
        if (nonNull(offset)) {
            // split can be empty if query = '\n'
            var split = query.split("\n");
            return message
                    + positionString
                    + lineSeparator()
                    + findErrorLine(offset, split.length != 0 ? split : new String[] {""});
        } else {
            return message;
        }
    }

    public static String findErrorLine(int offset, String[] message) {
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

        var nbrOfCarriageReturnsBeforeError = 0;
        var elementBeforeError = element.substring(0, currentOffset);
        Pattern pattern = Pattern.compile("\r");
        Matcher matcher = pattern.matcher(elementBeforeError);
        while (matcher.find()) {
            nbrOfCarriageReturnsBeforeError++;
        }

        builder.append("\"")
                .append(element.stripTrailing().replace("\r", "\\r")) // stripTrailing() removes potential \r at the end
                .append("\"")
                .append(lineSeparator())
                // extra space to compensate for an opening quote and printed out carriage returns as these have width
                // two
                .append(" ".repeat(currentOffset + 1 + nbrOfCarriageReturnsBeforeError))
                .append('^');
    }
}
