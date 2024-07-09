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
package org.neo4j.internal.batchimport;

import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.PropertyValueLookup;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.staging.LonelyProcessingStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.internal.helpers.progress.Indicator;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;

/**
 * Preparation of an {@link IdMapper}, {@link IdMapper#prepare(PropertyValueLookup, Collector, ProgressMonitorFactory)}
 * under running as a normal {@link Step} so that normal execution monitoring can be applied.
 * Useful since preparing an {@link IdMapper} can take a significant amount of time.
 */
public class IdMapperPreparationStep extends LonelyProcessingStep {
    private final IdMapper idMapper;
    private final PropertyValueLookup allIds;
    private final Collector collector;

    public IdMapperPreparationStep(
            StageControl control,
            Configuration config,
            IdMapper idMapper,
            PropertyValueLookup allIds,
            Collector collector,
            StatsProvider... additionalStatsProviders) {
        super(control, "" /*named later in the progress listener*/, config, additionalStatsProviders);
        this.idMapper = idMapper;
        this.allIds = allIds;
        this.collector = collector;
    }

    @Override
    protected void process() {
        idMapper.prepare(allIds, collector, new ProgressMonitorFactory() {
            private long totalCount;

            @Override
            protected Indicator newIndicator(String process) {
                int reportResolution = 100;
                return new Indicator(reportResolution) {
                    @Override
                    protected void progress(int from, int to) {
                        int steps = to - from;
                        IdMapperPreparationStep.this.progress(
                                (long) (totalCount * (steps / (double) reportResolution)));
                    }
                };
            }

            @Override
            public ProgressListener singlePart(String process, long totalCount) {
                this.totalCount = totalCount;
                return super.singlePart(process, totalCount);
            }
        });
    }
}
