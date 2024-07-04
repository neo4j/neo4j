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
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.SIMILARITY_FUNCTION;

import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.set.sorted.ImmutableSortedSet;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexConfigValidationWrapper;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.vector.VectorQuantization;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;

public class VectorIndexConfig extends IndexConfigValidationWrapper {
    private final int dimensions;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorQuantization quantization;

    VectorIndexConfig(
            IndexProviderDescriptor descriptor,
            IndexConfig config,
            ImmutableSortedMap<IndexSetting, Object> settings,
            ImmutableSortedSet<String> validSettingNames,
            ImmutableSortedSet<String> possibleValidSettingNames) {
        super(descriptor, config, settings, validSettingNames, possibleValidSettingNames);
        this.dimensions = get(DIMENSIONS);
        this.similarityFunction = get(SIMILARITY_FUNCTION);
        this.quantization = get(QUANTIZATION);
    }

    public int dimensions() {
        return dimensions;
    }

    public VectorSimilarityFunction similarityFunction() {
        return similarityFunction;
    }

    public VectorQuantization quantization() {
        return quantization;
    }
}
