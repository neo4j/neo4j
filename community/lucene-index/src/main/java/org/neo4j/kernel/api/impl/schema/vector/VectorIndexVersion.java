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

import java.util.Comparator;
import java.util.Locale;
import java.util.OptionalInt;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.SortedMaps;
import org.eclipse.collections.api.factory.primitive.BooleanSets;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.set.SetIterable;
import org.eclipse.collections.api.set.primitive.BooleanSet;
import org.eclipse.collections.api.set.primitive.ImmutableBooleanSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.IndexSettingValidators.IntegerValidator;
import org.neo4j.kernel.api.impl.schema.vector.IndexSettingValidators.OptionalIntSettingValidator;
import org.neo4j.kernel.api.impl.schema.vector.IndexSettingValidators.QuantizationEnabledValidator;
import org.neo4j.kernel.api.impl.schema.vector.IndexSettingValidators.ReadDefaultOnly;
import org.neo4j.kernel.api.impl.schema.vector.IndexSettingValidators.SimilarityFunctionValidator;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.Range;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexSettingsValidator.ValidatorNotFound;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexSettingsValidator.ValidatorNotFoundForKernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexSettingsValidator.Validators;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.Value;

public enum VectorIndexVersion {
    UNKNOWN(null, KernelVersion.EARLIEST, 0, 0, 0, Sets.immutable.empty(), BooleanSets.immutable.empty()) {
        @Override
        protected RichIterable<Pair<KernelVersion, VectorIndexSettingsValidator>> configureValidators() {
            return Lists.mutable.of(Tuples.pair(
                    KernelVersion.EARLIEST,
                    new ValidatorNotFound(new IllegalStateException("%s not found for '%s'"
                            .formatted(
                                    VectorIndexSettingsValidator.class.getSimpleName(),
                                    descriptor().name())))));
        }

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return false;
        }
    },

    V1_0(
            "1.0",
            KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED,
            2048,
            512,
            3200,
            Sets.mutable.of(VectorSimilarityFunctions.EUCLIDEAN, VectorSimilarityFunctions.SIMPLE_COSINE),
            BooleanSets.immutable.empty()) {
        @Override
        protected RichIterable<Pair<KernelVersion, VectorIndexSettingsValidator>> configureValidators() {
            return Lists.mutable.of(
                    Tuples.pair(
                            KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED,
                            new Validators(
                                    this,
                                    new OptionalIntSettingValidator(
                                            IndexSetting.vector_Dimensions(),
                                            new Range<>(1, Integer.MAX_VALUE)), // this was a bug
                                    new SimilarityFunctionValidator(nameToSimilarityFunction()),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Quantization_Enabled(), false),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Hnsw_M(), 16),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Hnsw_Ef_Construction(), 100))),
                    Tuples.pair(
                            KernelVersion.V5_12,
                            new Validators(
                                    this,
                                    new OptionalIntSettingValidator(
                                            IndexSetting.vector_Dimensions(), new Range<>(1, maxDimensions())),
                                    new SimilarityFunctionValidator(nameToSimilarityFunction()),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Quantization_Enabled(), false),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Hnsw_M(), 16),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Hnsw_Ef_Construction(), 100))));
        }

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return candidate instanceof FloatingPointArray;
        }
    },

    V2_0(
            "2.0",
            KernelVersion.VERSION_VECTOR_2_INTRODUCED,
            4096,
            512,
            3200,
            Sets.mutable.of(VectorSimilarityFunctions.EUCLIDEAN, VectorSimilarityFunctions.L2_NORM_COSINE),
            BooleanSets.immutable.of(false, true)) {
        @Override
        protected RichIterable<Pair<KernelVersion, VectorIndexSettingsValidator>> configureValidators() {
            return Lists.mutable.of(
                    Tuples.pair(
                            KernelVersion.VERSION_VECTOR_2_INTRODUCED,
                            new Validators(
                                    this,
                                    new OptionalIntSettingValidator(
                                            IndexSetting.vector_Dimensions(), new Range<>(1, maxDimensions())),
                                    new SimilarityFunctionValidator(nameToSimilarityFunction()),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Quantization_Enabled(), false),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Hnsw_M(), 16),
                                    new ReadDefaultOnly<>(IndexSetting.vector_Hnsw_Ef_Construction(), 100))),
                    Tuples.pair(
                            KernelVersion.VERSION_VECTOR_QUANTIZATION_AND_HYPER_PARAMS,
                            new Validators(
                                    this,
                                    new OptionalIntSettingValidator(
                                            IndexSetting.vector_Dimensions(),
                                            new Range<>(1, maxDimensions()),
                                            OptionalInt.empty()),
                                    new SimilarityFunctionValidator(
                                            nameToSimilarityFunction(), VectorSimilarityFunctions.L2_NORM_COSINE),
                                    new QuantizationEnabledValidator(supportedQuantizationBooleans(), false, true),
                                    new IntegerValidator(IndexSetting.vector_Hnsw_M(), 16, new Range<>(1, maxHnswM())),
                                    new IntegerValidator(
                                            IndexSetting.vector_Hnsw_Ef_Construction(),
                                            100,
                                            new Range<>(1, maxHnswEfConstruction())))));
        }

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return candidate instanceof NumberArray;
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
    private final ImmutableBooleanSet quantizationBooleans;
    private final int maxHnswM;
    private final int maxHnswEfConstruction;
    private final ImmutableSortedMap<KernelVersion, VectorIndexSettingsValidator> validators;
    private final VectorIndexSettingsValidator latestIndexSettingValidator;

    VectorIndexVersion(
            String version,
            KernelVersion minimumRequiredKernelVersion,
            int maxDimensions,
            int maxHnswM,
            int maxHnswEfConstruction,
            SetIterable<VectorSimilarityFunction> supportedSimilarityFunctions,
            BooleanSet supportedQuantizationEnableds) {
        this.minimumRequiredKernelVersion = minimumRequiredKernelVersion;
        this.descriptor = version != null
                ? new IndexProviderDescriptor(DESCRIPTOR_KEY, version)
                : IndexProviderDescriptor.UNDECIDED;

        this.maxDimensions = maxDimensions;
        this.similarityFunctions = supportedSimilarityFunctions.toImmutableMap(
                similarityFunction -> similarityFunction.name().toUpperCase(Locale.ROOT),
                similarityFunction -> similarityFunction);
        this.quantizationBooleans = supportedQuantizationEnableds.toImmutable();
        this.maxHnswM = maxHnswM;
        this.maxHnswEfConstruction = maxHnswEfConstruction;

        this.validators = SortedMaps.mutable
                .<KernelVersion, VectorIndexSettingsValidator>of(Comparator.reverseOrder())
                .withAllKeyValues(configureValidators())
                .toImmutable();
        this.latestIndexSettingValidator = indexSettingValidator(KernelVersion.getLatestVersion(Config.defaults()));
    }

    public KernelVersion minimumRequiredKernelVersion() {
        return minimumRequiredKernelVersion;
    }

    public IndexProviderDescriptor descriptor() {
        return descriptor;
    }

    @VisibleForTesting
    public int maxDimensions() {
        return maxDimensions;
    }

    @VisibleForTesting
    public int maxHnswM() {
        return maxHnswM;
    }

    @VisibleForTesting
    public int maxHnswEfConstruction() {
        return maxHnswEfConstruction;
    }

    protected abstract RichIterable<Pair<KernelVersion, VectorIndexSettingsValidator>> configureValidators();

    public abstract boolean acceptsValueInstanceType(Value candidate);

    public VectorSimilarityFunction maybeSimilarityFunction(String name) {
        return similarityFunctions.get(name.toUpperCase(Locale.ROOT));
    }

    public VectorSimilarityFunction similarityFunction(String name) {
        final var similarityFunction = maybeSimilarityFunction(name);
        if (similarityFunction == null) {
            throw new IllegalArgumentException(
                    "'%s' is an unsupported vector similarity function for index with provider %s. "
                                    .formatted(name, descriptor.name())
                            + "Supported: "
                            + similarityFunctions.keysView());
        }

        return similarityFunction;
    }

    @VisibleForTesting
    public RichIterable<VectorSimilarityFunction> supportedSimilarityFunctions() {
        return similarityFunctions.valuesView();
    }

    ImmutableMap<String, VectorSimilarityFunction> nameToSimilarityFunction() {
        return similarityFunctions;
    }

    @VisibleForTesting
    public ImmutableBooleanSet supportedQuantizationBooleans() {
        return quantizationBooleans;
    }

    public VectorIndexSettingsValidator indexSettingValidator() {
        return latestIndexSettingValidator;
    }

    public VectorIndexSettingsValidator indexSettingValidator(KernelVersion kernelVersion) {
        final var validator = validators
                .keyValuesView()
                .detect(kernelVersionAndValidator -> kernelVersion.isAtLeast(kernelVersionAndValidator.getOne()));
        if (validator == null) {
            return new ValidatorNotFoundForKernelVersion(this, kernelVersion);
        }

        return validator.getTwo();
    }
}
