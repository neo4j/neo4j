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

import static org.neo4j.gqlstatus.Condition.createStandardDescription;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *  Implementation of CommonGqlStatusObject interface
 *  Contains common implementations for notifications, standard codes and errors without exposing GqlStatusInfo
 *  and DiagnosticRecord as public API.
 */
public class CommonGqlStatusObjectImplementation implements CommonGqlStatusObject {

    protected final GqlStatusInfo gqlStatusInfo;
    protected final DiagnosticRecord diagnosticRecord;
    protected String messageWithParameters;

    public CommonGqlStatusObjectImplementation(
            GqlStatusInfo gqlStatusInfo, DiagnosticRecord diagnosticRecord, Object[] messageParameterValues) {
        this.gqlStatusInfo = gqlStatusInfo;
        this.diagnosticRecord = diagnosticRecord;
        this.messageWithParameters = insertMessageParameters(messageParameterValues);
    }

    public CommonGqlStatusObjectImplementation(
            GqlStatusInfo gqlStatusInfo,
            DiagnosticRecord diagnosticRecord,
            Map<GqlMessageParams, Object> messageParameters) {
        this.gqlStatusInfo = gqlStatusInfo;
        this.diagnosticRecord = diagnosticRecord;
        this.messageWithParameters = insertMessageParameters(messageParameters);
    }

    public CommonGqlStatusObjectImplementation(GqlStatusInfo gqlStatusInfo, DiagnosticRecord diagnosticRecord) {
        this.gqlStatusInfo = gqlStatusInfo;
        this.diagnosticRecord = diagnosticRecord;
        this.messageWithParameters = gqlStatusInfo.getMessage();
    }

    @Override
    public String gqlStatus() {
        return gqlStatusInfo.getStatusString();
    }

    @Override
    public String statusDescription() {
        var condition = gqlStatusInfo.getCondition();
        var subCondition = gqlStatusInfo.getSubCondition();

        if (messageWithParameters.isEmpty()) {
            return (createStandardDescription(condition, subCondition));
        }

        return (createStandardDescription(condition, subCondition)) + ". " + messageWithParameters;
    }

    @Override
    public Map<String, Object> diagnosticRecord() {
        return diagnosticRecord.asMap();
    }

    /*
     * Insert the message parameter values in the message and
     * create a statusParameters map for the diagnostic record
     */
    protected String insertMessageParameters(Object[] parameterValues) {
        String[] parameterKeys = gqlStatusInfo.getStatusParameterKeys();
        Map<String, Object> statusParameters = new HashMap<>();
        var messageParameters = new String[parameterValues.length];

        if (parameterKeys.length != parameterValues.length) {
            throw new IllegalArgumentException(String.format(
                    "Expected parameterKeys: %s and parameterValues: %s to have the same length.",
                    Arrays.toString(parameterKeys), Arrays.toString(parameterValues)));
        }

        for (int i = 0; i < parameterKeys.length; i++) {
            String key = parameterKeys[i];
            Object value = parameterValues[i];
            statusParameters.put(key, value);
            if (value == null) {
                messageParameters[i] = "null";
            } else if (value instanceof String s) {
                messageParameters[i] = s;
            } else if (value instanceof Boolean b) {
                messageParameters[i] = b.toString();
            } else if (value instanceof Integer nbr) {
                messageParameters[i] = nbr.toString();
            } else if (isListOfString(value)) {
                //noinspection unchecked
                messageParameters[i] = String.join(", ", ((List<String>) value));
            } else {
                throw new IllegalArgumentException(String.format(
                        "Expected parameter to be String, Boolean, Integer or List<String> but was %s", value));
            }
        }
        diagnosticRecord.setStatusParameters(statusParameters);
        return gqlStatusInfo.getMessage(List.of(messageParameters));
    }

    /*
     * Insert the message parameter values in the message and
     * check the types of the statusParameters map for the diagnostic record
     */
    protected String insertMessageParameters(Map<GqlMessageParams, Object> parameters) {
        var messageParameters = new HashMap<GqlMessageParams, String>();
        var statusParameters = new HashMap<String, Object>();

        for (var entry : parameters.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            statusParameters.put(key.name(), value);

            if (value == null) {
                messageParameters.put(key, "null");
            } else if (value instanceof String s) {
                messageParameters.put(key, s);
            } else if (value instanceof Boolean b) {
                messageParameters.put(key, b.toString());
            } else if (value instanceof Integer nbr) {
                messageParameters.put(key, nbr.toString());
            } else if (isListOfString(value)) {
                //noinspection unchecked
                messageParameters.put(key, String.join(", ", ((List<String>) value)));
            } else {
                throw new IllegalArgumentException(String.format(
                        "Expected parameter to be String, Boolean, Integer or List<String> but was %s", value));
            }
        }
        diagnosticRecord.setStatusParameters(statusParameters);
        return gqlStatusInfo.getMessage(messageParameters);
    }

    private boolean isListOfString(Object obj) {
        if (!(obj instanceof List<?> list)) {
            return false;
        }

        for (Object element : list) {
            if (!(element instanceof String)) {
                return false;
            }
        }
        return true;
    }

    public Condition getCondition() {
        return gqlStatusInfo.getCondition();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommonGqlStatusObjectImplementation that = (CommonGqlStatusObjectImplementation) o;
        return Objects.equals(gqlStatusInfo, that.gqlStatusInfo)
                && Objects.equals(diagnosticRecord, that.diagnosticRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gqlStatusInfo, diagnosticRecord);
    }
}
