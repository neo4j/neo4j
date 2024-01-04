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

import java.util.Locale;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.Value;

public enum VectorIndexVersion {
    UNKNOWN(null, KernelVersion.EARLIEST, 0) {
        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return false;
        }
    },

    V1_0(
            "1.0",
            KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED,
            2048,
            VectorSimilarityFunctions.EUCLIDEAN,
            VectorSimilarityFunctions.SIMPLE_COSINE) {

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return candidate instanceof FloatingPointArray;
        }
    };

    public static final ImmutableList<VectorIndexVersion> KNOWN_VERSIONS =
            Lists.mutable.with(values()).without(UNKNOWN).toImmutableList();

    public static VectorIndexVersion latestSupportedVersion(KernelVersion kernelVersion) {
        for (final var version : KNOWN_VERSIONS.asReversed()) {
            if (kernelVersion.isAtLeast(version.minimumRequiredKernelVersion)) {
                return version;
            }
        }
        return UNKNOWN;
    }

    public static VectorIndexVersion fromDescriptor(IndexProviderDescriptor descriptor) {
        for (final var version : KNOWN_VERSIONS.asReversed()) {
            if (version.descriptor.equals(descriptor)) {
                return version;
            }
        }
        return UNKNOWN;
    }

    private static final String DESCRIPTOR_KEY = "vector";

    private final KernelVersion minimumRequiredKernelVersion;
    private final IndexProviderDescriptor descriptor;
    private final int maxDimensions;
    private final ImmutableMap<String, VectorSimilarityFunction> similarityFunctions;

    VectorIndexVersion(
            String version,
            KernelVersion minimumRequiredKernelVersion,
            int maxDimensions,
            VectorSimilarityFunction... supportedSimilarityFunctions) {
        this.minimumRequiredKernelVersion = minimumRequiredKernelVersion;
        this.descriptor = version != null
                ? new IndexProviderDescriptor(DESCRIPTOR_KEY, version)
                : IndexProviderDescriptor.UNDECIDED;

        this.maxDimensions = maxDimensions;

        final var similarityFunctions =
                Maps.mutable.<String, VectorSimilarityFunction>withInitialCapacity(supportedSimilarityFunctions.length);
        for (final var similarityFunction : supportedSimilarityFunctions) {
            similarityFunctions.put(similarityFunction.name().toUpperCase(Locale.ROOT), similarityFunction);
        }
        this.similarityFunctions = similarityFunctions.toImmutable();
    }

    public KernelVersion minimumRequiredKernelVersion() {
        return minimumRequiredKernelVersion;
    }

    public IndexProviderDescriptor descriptor() {
        return descriptor;
    }

    public int maxDimensions() {
        return maxDimensions;
    }

    public abstract boolean acceptsValueInstanceType(Value candidate);

    public VectorSimilarityFunction similarityFunction(String name) {
        final var similarityFunction = similarityFunctions.get(name.toUpperCase(Locale.ROOT));
        if (similarityFunction == null) {
            throw new IllegalArgumentException("'%s' is an unsupported vector similarity function for %s. Supported: %s"
                    .formatted(name, descriptor.name(), similarityFunctions.keysView()));
        }

        return similarityFunction;
    }

    @VisibleForTesting
    public RichIterable<VectorSimilarityFunction> supportedSimilarityFunctions() {
        return similarityFunctions.valuesView();
    }
}
