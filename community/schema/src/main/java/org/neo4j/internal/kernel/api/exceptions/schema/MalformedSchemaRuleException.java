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
package org.neo4j.internal.kernel.api.exceptions.schema;

import java.util.Collections;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.Value;

/**
 * Signals that a schema rule in the schema store was malformed, i.e. contained corrupted data and could not
 * be parsed.
 */
public class MalformedSchemaRuleException extends SchemaKernelException {

    private MalformedSchemaRuleException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, Status.General.SchemaCorruptionDetected, message, cause);
    }

    private MalformedSchemaRuleException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, Status.General.SchemaCorruptionDetected, message);
    }

    public static MalformedSchemaRuleException internalError(String msgTitle, String message) {
        ErrorGqlStatusObject gql = GqlHelper.get50N00(msgTitle, message);
        return new MalformedSchemaRuleException(gql, message);
    }

    public static MalformedSchemaRuleException internalError(String msgTitle, String message, Throwable cause) {
        ErrorGqlStatusObject gql = GqlHelper.get50N00(msgTitle, message);
        return new MalformedSchemaRuleException(gql, message, cause);
    }

    public static MalformedSchemaRuleException propertyTypeMismatch(
            String property, Value value, Class<? extends Value> expectedType) {
        if (value == null) {
            return propertyUnexpectedlyNull(property, expectedType);
        }
        ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                .withParam(GqlParams.StringParam.value, value.prettyPrint())
                .withParam(GqlParams.ListParam.valueTypeList, Collections.singletonList(expectedType.getSimpleName()))
                .withParam(GqlParams.StringParam.valueType, value.getClass().getSimpleName())
                .build();
        String legacyMessage = String.format(
                "Expected property %s to be a %s but was %s", property, expectedType.getSimpleName(), value);
        return new MalformedSchemaRuleException(gql, legacyMessage);
    }

    public static MalformedSchemaRuleException propertyUnexpectedlyNull(
            String property, Class<? extends Value> expectedType) {
        ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                .withParam(GqlParams.StringParam.value, "null")
                .withParam(GqlParams.ListParam.valueTypeList, Collections.singletonList(expectedType.getSimpleName()))
                .withParam(GqlParams.StringParam.valueType, "null")
                .build();
        String legacyMessage = String.format(
                "Expected property %s to be a %s but was %s", property, expectedType.getSimpleName(), null);
        return new MalformedSchemaRuleException(gql, legacyMessage);
    }
}
