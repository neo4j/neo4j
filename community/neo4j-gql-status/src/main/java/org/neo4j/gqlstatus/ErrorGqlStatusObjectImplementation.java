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
package org.neo4j.gqlstatus;

import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ErrorGqlStatusObjectImplementation extends CommonGqlStatusObjectImplementation
        implements ErrorGqlStatusObject {
    private boolean isCause = false;
    private ErrorGqlStatusObject cause;
    private final Map<GqlParams.GqlParam, Object> paramMap;
    private final GqlStatusInfoCodes gqlStatusInfoCode;

    private ErrorGqlStatusObjectImplementation(
            GqlStatusInfoCodes gqlStatusInfoCode,
            Map<GqlParams.GqlParam, Object> parameters,
            ErrorGqlStatusObject cause,
            DiagnosticRecord diagnosticRecord) {
        super(gqlStatusInfoCode, diagnosticRecord, parameters);
        this.gqlStatusInfoCode = gqlStatusInfoCode;
        this.cause = cause;
        this.paramMap = replaceNulls(parameters);
    }

    // Decrease the risk of NullPointers in errors
    private Map<GqlParams.GqlParam, Object> replaceNulls(Map<GqlParams.GqlParam, Object> parameters) {
        return parameters.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> e.getValue() == null ? "null" : e.getValue()));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ErrorGqlStatusObjectImplementation gql
                && Objects.equals(gqlStatusInfoCode, gql.gqlStatusInfoCode)
                && Objects.equals(diagnosticRecord, gql.diagnosticRecord)
                && Objects.equals(cause, gql.cause)
                && Objects.equals(paramMap, gql.paramMap)) {
            return true;
        } else {
            return false;
        }
    }

    public static Builder from(GqlStatusInfoCodes gqlStatusInfo) {
        return new Builder(gqlStatusInfo);
    }

    @Override
    public Optional<ErrorGqlStatusObject> cause() {
        return Optional.ofNullable(cause);
    }

    public boolean isCause() {
        return isCause;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return this;
    }

    public void setCause(ErrorGqlStatusObject cause) {
        this.cause = cause;
    }

    public void markAsCause() {
        isCause = true;
    }

    @Override
    public void adjustPosition(int oldLine, int oldColumn, int oldOffset, int newLine, int newCol, int newOffset) {
        super.adjustPosition(oldLine, oldColumn, oldOffset, newLine, newCol, newOffset);
        cause().ifPresent(gqlStatusObjectCause -> {
            if (gqlStatusObjectCause instanceof ErrorGqlStatusObjectImplementation errorGqlStatusObjectImplementation) {
                // Recursive call for the chain of causes
                errorGqlStatusObjectImplementation.adjustPosition(
                        oldLine, oldColumn, oldOffset, newLine, newCol, newOffset);
            }
        });
    }

    @Override
    public String getMessage() {
        String gqlMessagePart = this.gqlStatusInfoCode.getMessage(paramMap);
        if (!gqlMessagePart.isEmpty()) {
            return String.format("%s: %s", gqlStatus(), gqlMessagePart);
        }
        return gqlStatus();
    }

    @Override
    public String legacyMessage() {
        return "";
    }

    @Override
    public String toString() {
        return recToString();
    }

    private String recToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("Status: ");
        sb.append(gqlStatusInfoCode.getStatusString().trim());
        sb.append("\n");
        sb.append("Message: ");
        sb.append(insertMessageParameters(paramMap).trim());
        sb.append("\n");
        sb.append("Subcondition: ");
        sb.append(gqlStatusInfoCode.getSubCondition().trim());
        if (cause != null) {
            sb.append("\n");
            sb.append("Caused by:");

            return sb.append(indent(4, cause.toString())).toString();
        } else {
            return sb.toString();
        }
    }

    public static String indent(int n, String input) {
        String indent = " ".repeat(n);
        String[] lines = input.split("\n");

        StringBuilder sb = new StringBuilder();

        for (String line : lines) {
            sb.append(indent).append(line).append("\n");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    public static class Builder {
        private ErrorGqlStatusObject cause = null;
        private Map<GqlParams.GqlParam, Object> paramMap = new HashMap<>();
        private final GqlStatusInfoCodes gqlStatusInfoCode;
        private final DiagnosticRecord.Builder diagnosticRecordBuilder = DiagnosticRecord.from();

        private Builder(GqlStatusInfoCodes gqlStatusInfo) {
            this.gqlStatusInfoCode = gqlStatusInfo;
        }

        public Builder withParam(GqlParams.StringParam param, String value) {
            this.paramMap.put(param, value);
            return this;
        }

        public Builder withParam(GqlParams.BooleanParam param, boolean value) {
            this.paramMap.put(param, value);
            return this;
        }

        public Builder withParam(GqlParams.NumberParam param, Number value) {
            this.paramMap.put(param, value);
            return this;
        }

        public Builder withParam(GqlParams.ListParam param, List<?> value) {
            this.paramMap.put(param, value);
            return this;
        }

        Builder withParamMap(Map<GqlParams.GqlParam, Object> paramMap) {
            this.paramMap = paramMap;
            return this;
        }

        public Builder withCause(ErrorGqlStatusObject cause) {
            if (cause instanceof ErrorGqlStatusObjectImplementation c) {
                c.markAsCause();
            }
            this.cause = cause;
            return this;
        }

        public Builder withClassification(GqlClassification classification) {
            // TODO: This is a no-op method which will be cleaned up after error migration is ready
            return this;
        }

        public Builder atPosition(int line, int col, int offset) {
            diagnosticRecordBuilder.atPosition(line, col, offset);
            return this;
        }

        public ErrorGqlStatusObject build() {
            diagnosticRecordBuilder.withClassification(gqlStatusInfoCode.getClassification());
            DiagnosticRecord diagnosticRecord = diagnosticRecordBuilder.build();
            return new ErrorGqlStatusObjectImplementation(gqlStatusInfoCode, paramMap, cause, diagnosticRecord);
        }
    }
}
