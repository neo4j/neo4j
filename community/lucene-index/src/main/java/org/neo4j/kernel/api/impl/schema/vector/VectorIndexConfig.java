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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.DIMENSIONS;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_EF_CONSTRUCTION;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_M;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_ENABLED;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.SIMILARITY_FUNCTION;

import java.util.Objects;
import java.util.OptionalInt;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.set.sorted.ImmutableSortedSet;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexConfigValidationWrapper;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;

public class VectorIndexConfig extends IndexConfigValidationWrapper {
    private final VectorIndexVersion version;
    private final OptionalInt dimensions;
    private final VectorSimilarityFunction similarityFunction;
    private final boolean quantizationEnabled;
    private final HnswConfig hnswConfig;

    VectorIndexConfig(
            VectorIndexVersion version,
            IndexConfig config,
            ImmutableSortedMap<IndexSetting, Object> settings,
            ImmutableSortedSet<String> validSettingNames,
            ImmutableSortedSet<String> possibleValidSettingNames) {
        super(version.descriptor(), config, settings, validSettingNames, possibleValidSettingNames);
        this.version = version;
        this.dimensions = get(DIMENSIONS);
        this.similarityFunction = get(SIMILARITY_FUNCTION);
        this.quantizationEnabled = get(QUANTIZATION_ENABLED);
        this.hnswConfig = new HnswConfig(get(HNSW_M), get(HNSW_EF_CONSTRUCTION));
    }

    public VectorIndexVersion version() {
        return version;
    }

    public OptionalInt dimensions() {
        return dimensions;
    }

    public VectorSimilarityFunction similarityFunction() {
        return similarityFunction;
    }

    public boolean quantizationEnabled() {
        return quantizationEnabled;
    }

    public HnswConfig hnsw() {
        return hnswConfig;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensions, similarityFunction, quantizationEnabled, hnswConfig);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VectorIndexConfig that)) {
            return false;
        }
        return Objects.equals(this.dimensions, that.dimensions)
                && Objects.equals(this.similarityFunction, that.similarityFunction)
                && this.quantizationEnabled == that.quantizationEnabled
                && Objects.equals(this.hnswConfig, that.hnswConfig);
    }

    public record HnswConfig(int M, int efConstruction) {
        public static final HnswConfig DUMMY = new HnswConfig(16, 100);
    }
}
