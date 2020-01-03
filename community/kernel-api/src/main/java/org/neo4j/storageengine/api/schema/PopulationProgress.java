/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api.schema;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.helpers.collection.Pair;

import static java.lang.String.format;

public interface PopulationProgress
{
    PopulationProgress NONE = single( 0, 0 );
    PopulationProgress DONE = single( 1, 1 );

    long getCompleted();

    long getTotal();

    float getProgress();

    IndexPopulationProgress toIndexPopulationProgress();

    static PopulationProgress single( long completed, long total )
    {
        return new PopulationProgress()
        {
            @Override
            public long getCompleted()
            {
                return completed;
            }

            @Override
            public long getTotal()
            {
                return total;
            }

            @Override
            public float getProgress()
            {
                return (total == 0) ? 0 : (float) ((double) completed / total);
            }

            @Override
            public IndexPopulationProgress toIndexPopulationProgress()
            {
                return new IndexPopulationProgress( completed, total );
            }

            @Override
            public String toString()
            {
                return format( "[%d/%d:%f]", completed, total, getProgress() );
            }
        };
    }

    static MultiBuilder multiple()
    {
        return new MultiBuilder();
    }

    class MultiBuilder
    {
        private final List<Pair<PopulationProgress,Float>> parts = new ArrayList<>();
        private float totalWeight;

        public MultiBuilder add( PopulationProgress part, float weight )
        {
            parts.add( Pair.of( part, weight ) );
            totalWeight += weight;
            return this;
        }

        public PopulationProgress build()
        {
            float[] weightFactors = buildWeightFactors();
            return new PopulationProgress()
            {
                @Override
                public long getCompleted()
                {
                    return parts.stream().mapToLong( part -> part.first().getCompleted() ).sum();
                }

                @Override
                public long getTotal()
                {
                    return parts.stream().mapToLong( part -> part.first().getTotal() ).sum();
                }

                @Override
                public float getProgress()
                {
                    float combined = 0;
                    for ( int i = 0; i < parts.size(); i++ )
                    {
                        combined += parts.get( i ).first().getProgress() * weightFactors[i];
                    }
                    return combined;
                }

                @Override
                public IndexPopulationProgress toIndexPopulationProgress()
                {
                    // Here we want to control the progress percentage and the best way to do that without introducing
                    // another IndexPopulationProgress constructor is to make up completed/total values that will generate
                    // the progress we want (nobody uses getCompleted()/getTotal() anyway since even the widely used IndexPopulationProgress#DONE)
                    // destroys any actual numbers by having 1/1.
                    float progress = getProgress();
                    long fakeTotal = 1_000; // because we have 4 value digits in the report there
                    long fakeCompleted = (long) ((float) fakeTotal * progress);
                    return new IndexPopulationProgress( fakeCompleted, fakeTotal );
                }
            };
        }

        private float[] buildWeightFactors()
        {
            float[] weightFactors = new float[parts.size()];
            float weightSum = 0;
            for ( int i = 0; i < parts.size(); i++ )
            {
                Pair<PopulationProgress,Float> part = parts.get( i );
                weightFactors[i] = i == parts.size() - 1 ? 1 - weightSum : part.other() / totalWeight;
                weightSum += weightFactors[i];
            }
            return weightFactors;
        }
    }
}
