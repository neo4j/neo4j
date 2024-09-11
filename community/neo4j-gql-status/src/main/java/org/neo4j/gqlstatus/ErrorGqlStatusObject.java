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

import static org.neo4j.gqlstatus.DiagnosticRecord.DEFAULT_DIAGNOSTIC_RECORD;

import java.util.Map;
import java.util.Optional;
import org.neo4j.annotations.api.PublicApi;

@PublicApi
public interface ErrorGqlStatusObject extends CommonGqlStatusObject {
    String DEFAULT_STATUS_CODE = GqlStatusInfoCodes.STATUS_50N42.getStatusString();

    String DEFAULT_STATUS_DESCRIPTION = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
            .build()
            .statusDescription();

    ErrorGqlStatusObject gqlStatusObject();

    String legacyMessage();

    @Override
    default String gqlStatus() {
        var innerGqlStatusObject = gqlStatusObject();
        if (innerGqlStatusObject != null) {
            return innerGqlStatusObject.gqlStatus();
        }
        return DEFAULT_STATUS_CODE;
    }

    @Override
    default String statusDescription() {
        var innerGqlStatusObject = gqlStatusObject();
        if (innerGqlStatusObject != null) {
            return innerGqlStatusObject.statusDescription();
        }
        return DEFAULT_STATUS_DESCRIPTION;
    }

    @Override
    default Map<String, Object> diagnosticRecord() {
        var innerGqlStatusObject = gqlStatusObject();
        if (innerGqlStatusObject != null) {

            return innerGqlStatusObject.diagnosticRecord();
        }
        return DEFAULT_DIAGNOSTIC_RECORD;
    }

    default Optional<ErrorGqlStatusObject> cause() {
        var innerGqlStatusObject = gqlStatusObject();
        if (innerGqlStatusObject != null) {
            return innerGqlStatusObject.cause();
        }
        return Optional.empty();
    }

    default ErrorClassification getClassification() {
        Object maybeClassification = diagnosticRecord().get("_classification");
        if (maybeClassification == null) {
            return ErrorClassification.UNKNOWN;
        } else {
            return ErrorClassification.valueOf((String) maybeClassification);
        }
    }
}
