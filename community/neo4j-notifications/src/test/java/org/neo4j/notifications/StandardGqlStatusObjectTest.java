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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.Condition;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.gqlstatus.NotificationClassification;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.SeverityLevel;

class StandardGqlStatusObjectTest {

    @Test
    void successShouldHaveExpectedFields() {
        StandardGqlStatusObject success = StandardGqlStatusObject.SUCCESS;
        assertThat(success.gqlStatus()).isEqualTo("00000");
        assertThat(success.getCondition()).isEqualTo(Condition.SUCCESSFUL_COMPLETION);
        assertThat(success.diagnosticRecord()).containsExactlyInAnyOrderEntriesOf(new DiagnosticRecord().asMap());
        assertThat(success.getPosition()).isEqualTo(InputPosition.empty);
        assertThat(success.getSeverity()).isEqualTo(SeverityLevel.UNKNOWN);
        assertThat(success.getClassification()).isEqualTo(NotificationClassification.UNKNOWN);
        assertThat(success.statusDescription()).isEqualTo("note: successful completion");
    }

    @Test
    void omittedResultShouldHaveExpectedFields() {
        StandardGqlStatusObject omittedResult = StandardGqlStatusObject.OMITTED_RESULT;
        assertThat(omittedResult.gqlStatus()).isEqualTo("00001");
        assertThat(omittedResult.getCondition()).isEqualTo(Condition.SUCCESSFUL_COMPLETION);
        assertThat(omittedResult.diagnosticRecord()).containsExactlyInAnyOrderEntriesOf(new DiagnosticRecord().asMap());
        assertThat(omittedResult.getPosition()).isEqualTo(InputPosition.empty);
        assertThat(omittedResult.getSeverity()).isEqualTo(SeverityLevel.UNKNOWN);
        assertThat(omittedResult.getClassification()).isEqualTo(NotificationClassification.UNKNOWN);
        assertThat(omittedResult.statusDescription()).isEqualTo("note: successful completion - omitted result");
    }

    @Test
    void noDataShouldHaveExpectedFields() {
        StandardGqlStatusObject noData = StandardGqlStatusObject.NO_DATA;
        assertThat(noData.gqlStatus()).isEqualTo("02000");
        assertThat(noData.getCondition()).isEqualTo(Condition.NO_DATA);
        assertThat(noData.diagnosticRecord()).containsExactlyInAnyOrderEntriesOf(new DiagnosticRecord().asMap());
        assertThat(noData.getPosition()).isEqualTo(InputPosition.empty);
        assertThat(noData.getSeverity()).isEqualTo(SeverityLevel.UNKNOWN);
        assertThat(noData.getClassification()).isEqualTo(NotificationClassification.UNKNOWN);
        assertThat(noData.statusDescription()).isEqualTo("note: no data");
    }

    @Test
    void unknownNoDataShouldHaveExpectedFields() {
        StandardGqlStatusObject noData = StandardGqlStatusObject.UNKNOWN_NO_DATA;
        assertThat(noData.gqlStatus()).isEqualTo("02N42");
        assertThat(noData.getCondition()).isEqualTo(Condition.NO_DATA);
        assertThat(noData.diagnosticRecord()).containsExactlyInAnyOrderEntriesOf(new DiagnosticRecord().asMap());
        assertThat(noData.getPosition()).isEqualTo(InputPosition.empty);
        assertThat(noData.getSeverity()).isEqualTo(SeverityLevel.UNKNOWN);
        assertThat(noData.getClassification()).isEqualTo(NotificationClassification.UNKNOWN);
        assertThat(noData.statusDescription())
                .isEqualTo("note: no data - unknown subcondition. Unknown GQLSTATUS from old server.");
    }
}
