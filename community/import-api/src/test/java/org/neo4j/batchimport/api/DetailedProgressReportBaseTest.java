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
package org.neo4j.batchimport.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Duration;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.batchimport.api.input.ResumableStateData;
import org.neo4j.batchimport.api.input.ResumableStateData.ResumableStateDataBuilder;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;

class DetailedProgressReportBaseTest {
    private static final TokenNameLookup TEST_TOKEN_NAME_LOOKUP = new TokenNameLookup() {
        @Override
        public String labelGetName(int labelId) {
            return "Label" + labelId;
        }

        @Override
        public String relationshipTypeGetName(int relationshipTypeId) {
            return "Type" + relationshipTypeId;
        }

        @Override
        public String propertyKeyGetName(int propertyKeyId) {
            return "Prop" + propertyKeyId;
        }
    };

    @Test
    void shouldResumeFromCheckpoint() {
        var original = populatedReportBase();
        ResumableStateData checkpoint = checkpointOf(original);

        var resumed = reportBaseWithNoStats();
        resumed.resumeFromCheckpoint(checkpoint);

        assertThat(resumed.resumableSnapshot()).isEqualTo(original.resumableSnapshot());
        assertThat(resumed.snapshot()).isEqualTo(original.snapshot());
    }

    @Test
    void shouldResumeFromCheckpointThroughSerializedForm() throws IOException {
        var original = populatedReportBase();
        ResumableStateData checkpoint = ResumableStateData.fromInputStream(new DataInputStream(
                new ByteArrayInputStream(checkpointOf(original).asByteArray())));

        var resumed = reportBaseWithNoStats();
        resumed.resumeFromCheckpoint(checkpoint);

        assertThat(resumed.resumableSnapshot()).isEqualTo(original.resumableSnapshot());
        assertThat(resumed.snapshot()).isEqualTo(original.snapshot());
    }

    @Test
    void shouldAccumulateRegistrationsOnTopOfResumedState() {
        var original = populatedReportBase();
        ResumableStateData checkpoint = checkpointOf(original);

        var resumed = reportBaseWithNoStats();
        resumed.resumeFromCheckpoint(checkpoint);

        resumed.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(1, 150));
        resumed.registerRelationshipStats(ApplicationMode.UPDATE, 7);
        resumed.registerIndexStats(ApplicationMode.CREATE, EntityType.NODE, IntSets.immutable.of(1));

        DetailedProgressReport originalReport = original.resumableSnapshot();
        DetailedProgressReport resumedReport = resumed.resumableSnapshot();
        assertThat(resumedReport.nodeStats().created())
                .isEqualTo(originalReport.nodeStats().created() + 1);
        assertThat(resumedReport.nodePerLabelStats().get("Label1").created())
                .isEqualTo(originalReport.nodePerLabelStats().get("Label1").created() + 1);
        assertThat(resumedReport.relationshipStats().updated())
                .isEqualTo(originalReport.relationshipStats().updated() + 1);
        assertThat(resumedReport.relationshipPerTypeStats().get("Type7").updated())
                .isEqualTo(1);
        assertThat(resumedReport.nodeIndexStats().created())
                .isEqualTo(originalReport.nodeIndexStats().created() + 1);
    }

    @Test
    void shouldKeepRestoredTimerDurationWhileTheTimerDoesNotRunAgain() {
        var original = populatedReportBase();
        ResumableStateData checkpoint = checkpointOf(original);
        Duration originalDuration = original.resumableSnapshot().relationshipRangeDivisionDuration();

        var resumed = new DetailedProgressReportBase(42, 69, true);
        resumed.resumeFromCheckpoint(checkpoint);

        assertThat(resumed.relationshipRangeDivisionTimer().snapshot()).isEqualTo(originalDuration);
    }

    @Test
    void shouldReplaceRestoredTimerDurationWhenTheTimerRunsAgain() throws InterruptedException {
        var original = populatedReportBase();
        ResumableStateData checkpoint = checkpointOf(original);

        var resumed = new DetailedProgressReportBase(42, 69, true);
        resumed.resumeFromCheckpoint(checkpoint);

        resumed.nodeIdMappingTimer().start();
        Thread.sleep(1);
        resumed.nodeIdMappingTimer().end();

        assertThat(resumed.nodeIdMappingTimer().snapshot().toNanos())
                .isNotEqualTo(checkpoint.progressReport().nodeIdMappingDurationNanos());
    }

    @Test
    void shouldTolerateCheckpointWithoutProgressReport() {
        var resumed = new DetailedProgressReportBase(42, 69, true);
        resumed.resumeFromCheckpoint(new ResumableStateDataBuilder().build());

        assertThat(resumed.resumableSnapshot().nodeStats()).isEqualTo(new DetailedProgressReport.Stats(0, 0, 0, 0));
    }

    @Test
    void shouldHaveNothingToResumeFromStart() {
        var resumed = new DetailedProgressReportBase(42, 69, true);
        resumed.resumeFromStart();

        assertThat(resumed.resumableSnapshot().nodeStats()).isEqualTo(new DetailedProgressReport.Stats(0, 0, 0, 0));
        assertThat(resumed.resumableSnapshot().nodePerLabelStats()).isEmpty();
    }

    @Test
    void timerShouldReportZeroBeforeAnythingHappened() {
        assertThat(new DetailedProgressReportBase.Timer().snapshot()).isEqualTo(Duration.ZERO);
    }

    private static ResumableStateData checkpointOf(DetailedProgressReportBase reportBase) {
        var builder = new ResumableStateDataBuilder();
        reportBase.checkpoint(builder);
        return builder.build();
    }

    private static DetailedProgressReportBase reportBaseWithNoStats() {
        var reportBase = new DetailedProgressReportBase(42, 69, true);
        reportBase.setTokenNameLookup(TEST_TOKEN_NAME_LOOKUP);
        return reportBase;
    }

    private static DetailedProgressReportBase populatedReportBase() {
        var reportBase = reportBaseWithNoStats();
        reportBase.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(1, 2));
        reportBase.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(1));
        reportBase.registerRelationshipStats(ApplicationMode.CREATE, 5);
        reportBase.registerIndexStats(ApplicationMode.CREATE, EntityType.NODE, IntSets.immutable.of(1));
        reportBase.registerConstraintStats(ApplicationMode.CREATE, EntityType.RELATIONSHIP, IntSets.immutable.of(5));
        reportBase.nodeIdMappingTimer().start();
        reportBase.nodeIdMappingTimer().end();
        reportBase.relationshipRangeDivisionTimer().start();
        reportBase.relationshipRangeDivisionTimer().end();
        reportBase.storeApplyingTimer().start();
        reportBase.storeApplyingTimer().end();
        reportBase.schemaTimer().start();
        reportBase.schemaTimer().end();
        return reportBase;
    }
}
