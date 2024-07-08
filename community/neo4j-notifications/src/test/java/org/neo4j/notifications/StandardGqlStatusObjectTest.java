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
package org.neo4j.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.Condition;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.NotificationClassification;
import org.neo4j.graphdb.SeverityLevel;

class StandardGqlStatusObjectTest {

    @Test
    void successShouldHaveExpectedFields() {
        StandardGqlStatusObject success = StandardGqlStatusObject.SUCCESS;
        assertEquals("00000", success.gqlStatus());
        assertEquals(Condition.SUCCESSFUL_COMPLETION, success.getCondition());
        assertEquals(new DiagnosticRecord().asMap(), success.diagnosticRecord());
        assertEquals(InputPosition.empty, success.getPosition());
        assertEquals(SeverityLevel.UNKNOWN, success.getSeverity());
        assertEquals(NotificationClassification.UNKNOWN, success.getClassification());
        assertEquals("note: successful completion", success.statusDescription());
    }

    @Test
    void omittedResultShouldHaveExpectedFields() {
        StandardGqlStatusObject omittedResult = StandardGqlStatusObject.OMITTED_RESULT;
        assertEquals("00001", omittedResult.gqlStatus());
        assertEquals(Condition.SUCCESSFUL_COMPLETION, omittedResult.getCondition());
        assertEquals(new DiagnosticRecord().asMap(), omittedResult.diagnosticRecord());
        assertEquals(InputPosition.empty, omittedResult.getPosition());
        assertEquals(SeverityLevel.UNKNOWN, omittedResult.getSeverity());
        assertEquals(NotificationClassification.UNKNOWN, omittedResult.getClassification());
        assertEquals("note: successful completion - omitted result", omittedResult.statusDescription());
    }

    @Test
    void noDataShouldHaveExpectedFields() {
        StandardGqlStatusObject noData = StandardGqlStatusObject.NO_DATA;
        assertEquals("02000", noData.gqlStatus());
        assertEquals(Condition.NO_DATA, noData.getCondition());
        assertEquals(new DiagnosticRecord().asMap(), noData.diagnosticRecord());
        assertEquals(InputPosition.empty, noData.getPosition());
        assertEquals(SeverityLevel.UNKNOWN, noData.getSeverity());
        assertEquals(NotificationClassification.UNKNOWN, noData.getClassification());
        assertEquals("note: no data", noData.statusDescription());
    }

    @Test
    void unknownNoDataShouldHaveExpectedFields() {
        StandardGqlStatusObject noData = StandardGqlStatusObject.UNKNOWN_NO_DATA;
        assertEquals("02N42", noData.gqlStatus());
        assertEquals(Condition.NO_DATA, noData.getCondition());
        assertEquals(new DiagnosticRecord().asMap(), noData.diagnosticRecord());
        assertEquals(InputPosition.empty, noData.getPosition());
        assertEquals(SeverityLevel.UNKNOWN, noData.getSeverity());
        assertEquals(NotificationClassification.UNKNOWN, noData.getClassification());
        assertEquals(
                "note: no data - unknown subcondition. Unknown GQLSTATUS from old server.", noData.statusDescription());
    }
}
