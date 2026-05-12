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

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Map.entry;
import static org.neo4j.internal.schema.SequencedIndexSettingProcessors.mergeToValidatingProcessor;
import static org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction.EUCLIDEAN;
import static org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction.L2_NORM_COSINE;
import static org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction.SIMPLE_COSINE;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.DEFAULT_SEARCH_EXPANSION_FACTOR_EXTRACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.DIMENSIONS_EXTRACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_EF_CONSTRUCTION_EXTRACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_M_EXTRACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.OPTIONAL_DIMENSION_CONVERTER;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.OPTIONAL_QUANTIZATION_ENABLED_CONVERTER;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_ENABLED_EXTRACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_ENABLED_VALIDATOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_TYPE_EXTRACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_TYPE_UPPER_CASE_CONVERTER;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.REMOVE_QUANTIZATION_ENABLED;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.SIMILARITY_FUNCTION_EXTRACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.defaultSearchExpansionFactor;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.defaultSearchExpansionFactorDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.defaultSearchExpansionFactorValidator;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.dimensionValidator;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.hnswEfConstruction;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.hnswEfConstructionDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.hnswEfConstructionValidator;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.hnswM;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.hnswMDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.hnswMValidator;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.optionalDimensionDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.optionalDimensionValidator;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.optionalQuantizationEnabledDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.quantizationEnabledDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.quantizationEnabledToTypeMigrator;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.quantizationType;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.quantizationTypeDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.quantizationTypeLookup;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.quantizationTypeNormalizer;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.similarityFunctionDefault;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.similarityFunctionLookup;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.similarityFunctionNormalizer;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.DefaultIndexSettingsValidator;
import org.neo4j.internal.schema.DefaultIndexSettingsValidator.IndexSettingEntry;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexSettingExtractors;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsProcessor.ValidatingIndexSettingsProcessor;
import org.neo4j.internal.schema.NotFoundTypedIndexSettingsValidator;
import org.neo4j.internal.schema.TypedIndexSettingsValidator;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.VectorCandidate;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.Value;

public enum VectorIndexVersion {
    UNKNOWN(
            AllIndexProviderDescriptors.UNDECIDED,
            KernelVersion.EARLIEST,
            0,
            0,
            0,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()) {
        @Override
        protected Map<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> configureValidators() {
            return Map.ofEntries(entry(
                    KernelVersion.EARLIEST,
                    new NotFoundTypedIndexSettingsValidator<>(
                            AllIndexProviderDescriptors.UNDECIDED,
                            InvalidArgumentException.internalError(
                                    "Validator Not Found",
                                    "Validator not found for '%s'"
                                            .formatted(descriptor().name())))));
        }

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return false;
        }
    },

    V1_0(
            AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR,
            KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED,
            2048,
            512,
            3200,
            Set.of(EUCLIDEAN, SIMPLE_COSINE),
            Collections.emptySet(),
            Collections.emptySet()) {
        @Override
        protected Map<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> configureValidators() {
            return Map.ofEntries(
                    entry(
                            KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED,
                            new VersionedValidator(
                                    this,
                                    new IndexSettingExtractors(DIMENSIONS_EXTRACTOR, SIMILARITY_FUNCTION_EXTRACTOR),
                                    mergeToValidatingProcessor(
                                            dimensionValidator(1, Integer.MAX_VALUE), // this was a bug
                                            OPTIONAL_DIMENSION_CONVERTER,
                                            SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER,
                                            similarityFunctionLookup(nameToSimilarityFunction()),
                                            similarityFunctionNormalizer(nameToSimilarityFunction())),
                                    defaultSearchExpansionFactor(1.0),
                                    quantizationType(VectorQuantizationType.NONE),
                                    hnswM(16),
                                    hnswEfConstruction(100))),
                    entry(
                            KernelVersion.V5_12,
                            new VersionedValidator(
                                    this,
                                    new IndexSettingExtractors(DIMENSIONS_EXTRACTOR, SIMILARITY_FUNCTION_EXTRACTOR),
                                    mergeToValidatingProcessor(
                                            dimensionValidator(1, maxDimensions()),
                                            OPTIONAL_DIMENSION_CONVERTER,
                                            SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER,
                                            similarityFunctionLookup(nameToSimilarityFunction()),
                                            similarityFunctionNormalizer(nameToSimilarityFunction())),
                                    defaultSearchExpansionFactor(1.0),
                                    quantizationType(VectorQuantizationType.NONE),
                                    hnswM(16),
                                    hnswEfConstruction(100))));
        }

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return candidate instanceof FloatingPointArray;
        }
    },

    V2_0(
            AllIndexProviderDescriptors.VECTOR_V2_DESCRIPTOR,
            KernelVersion.VERSION_VECTOR_2_INTRODUCED,
            4096,
            512,
            3200,
            Set.of(EUCLIDEAN, L2_NORM_COSINE),
            Set.of(false, true),
            Collections.emptySet()) {
        @Override
        protected Map<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> configureValidators() {
            return Map.ofEntries(
                    entry(
                            KernelVersion.VERSION_VECTOR_2_INTRODUCED,
                            new VersionedValidator(
                                    this,
                                    new IndexSettingExtractors(DIMENSIONS_EXTRACTOR, SIMILARITY_FUNCTION_EXTRACTOR),
                                    mergeToValidatingProcessor(
                                            dimensionValidator(1, maxDimensions()),
                                            OPTIONAL_DIMENSION_CONVERTER,
                                            SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER,
                                            similarityFunctionLookup(nameToSimilarityFunction()),
                                            similarityFunctionNormalizer(nameToSimilarityFunction())),
                                    defaultSearchExpansionFactor(1.0),
                                    quantizationType(VectorQuantizationType.NONE),
                                    hnswM(16),
                                    hnswEfConstruction(100))),
                    entry(
                            KernelVersion.VERSION_VECTOR_QUANTIZATION_AND_HYPER_PARAMS,
                            new VersionedValidator(
                                    this,
                                    new IndexSettingExtractors(
                                            DIMENSIONS_EXTRACTOR,
                                            SIMILARITY_FUNCTION_EXTRACTOR,
                                            QUANTIZATION_ENABLED_EXTRACTOR,
                                            HNSW_M_EXTRACTOR,
                                            HNSW_EF_CONSTRUCTION_EXTRACTOR),
                                    mergeToValidatingProcessor(
                                            OPTIONAL_DIMENSION_CONVERTER,
                                            optionalDimensionDefault(OptionalInt.empty()),
                                            optionalDimensionValidator(1, maxDimensions()),
                                            SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER,
                                            similarityFunctionDefault(L2_NORM_COSINE),
                                            similarityFunctionLookup(nameToSimilarityFunction()),
                                            similarityFunctionNormalizer(nameToSimilarityFunction()),
                                            quantizationEnabledDefault(false, true),
                                            QUANTIZATION_ENABLED_VALIDATOR,
                                            quantizationEnabledToTypeMigrator(VectorQuantizationType.SCALAR),
                                            hnswMDefault(16),
                                            hnswMValidator(1, maxHnswM()),
                                            hnswEfConstructionDefault(100),
                                            hnswEfConstructionValidator(1, maxHnswEfConstruction())),
                                    defaultSearchExpansionFactor(1.0))));
        }

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return candidate instanceof VectorCandidate;
        }
    },
    V3_0(
            AllIndexProviderDescriptors.VECTOR_V3_DESCRIPTOR,
            KernelVersion.VERSION_LUCENE_10_INTRODUCED,
            4096,
            512,
            3200,
            Set.of(EUCLIDEAN, L2_NORM_COSINE),
            Set.of(false, true),
            Set.of(VectorQuantizationType.NONE, VectorQuantizationType.SCALAR, VectorQuantizationType.BINARY)) {
        @Override
        protected Map<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> configureValidators() {
            return Map.ofEntries(
                    entry(
                            KernelVersion.VERSION_LUCENE_10_INTRODUCED,
                            new VersionedValidator(
                                    this,
                                    new IndexSettingExtractors(
                                            DIMENSIONS_EXTRACTOR,
                                            SIMILARITY_FUNCTION_EXTRACTOR,
                                            QUANTIZATION_ENABLED_EXTRACTOR,
                                            HNSW_M_EXTRACTOR,
                                            HNSW_EF_CONSTRUCTION_EXTRACTOR),
                                    mergeToValidatingProcessor(
                                            OPTIONAL_DIMENSION_CONVERTER,
                                            optionalDimensionDefault(OptionalInt.empty()),
                                            optionalDimensionValidator(1, maxDimensions()),
                                            SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER,
                                            similarityFunctionDefault(L2_NORM_COSINE),
                                            similarityFunctionLookup(nameToSimilarityFunction()),
                                            similarityFunctionNormalizer(nameToSimilarityFunction()),
                                            quantizationEnabledDefault(false, true),
                                            QUANTIZATION_ENABLED_VALIDATOR,
                                            quantizationEnabledToTypeMigrator(VectorQuantizationType.SCALAR),
                                            hnswMDefault(16),
                                            hnswMValidator(1, maxHnswM()),
                                            hnswEfConstructionDefault(100),
                                            hnswEfConstructionValidator(1, maxHnswEfConstruction())),
                                    defaultSearchExpansionFactor(1.0))),
                    entry(
                            KernelVersion.VERSION_VECTOR_BINARY_QUANTIZATION,
                            new VersionedValidator(
                                    this,
                                    new IndexSettingExtractors(
                                            DIMENSIONS_EXTRACTOR,
                                            SIMILARITY_FUNCTION_EXTRACTOR,
                                            DEFAULT_SEARCH_EXPANSION_FACTOR_EXTRACTOR,
                                            QUANTIZATION_ENABLED_EXTRACTOR, // allowed in initial creation
                                            QUANTIZATION_TYPE_EXTRACTOR,
                                            HNSW_M_EXTRACTOR,
                                            HNSW_EF_CONSTRUCTION_EXTRACTOR),
                                    mergeToValidatingProcessor(
                                            OPTIONAL_DIMENSION_CONVERTER,
                                            optionalDimensionDefault(OptionalInt.empty()),
                                            optionalDimensionValidator(1, maxDimensions()),
                                            SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER,
                                            similarityFunctionDefault(L2_NORM_COSINE),
                                            similarityFunctionLookup(nameToSimilarityFunction()),
                                            similarityFunctionNormalizer(nameToSimilarityFunction()),
                                            OPTIONAL_QUANTIZATION_ENABLED_CONVERTER,
                                            optionalQuantizationEnabledDefault(Optional.empty()),
                                            QUANTIZATION_TYPE_UPPER_CASE_CONVERTER,
                                            quantizationTypeDefault(VectorQuantizationType.SCALAR),
                                            quantizationTypeLookup(supportedQuantizationTypes()),
                                            REMOVE_QUANTIZATION_ENABLED,
                                            quantizationTypeNormalizer(supportedQuantizationTypes()),
                                            defaultSearchExpansionFactorDefault(
                                                    1.0,
                                                    Map.ofEntries(
                                                            entry(VectorQuantizationType.NONE, 1.0),
                                                            entry(VectorQuantizationType.SCALAR, 2.0),
                                                            entry(VectorQuantizationType.BINARY, 8.0))),
                                            defaultSearchExpansionFactorValidator(1.0, Double.MAX_VALUE),
                                            hnswMDefault(16),
                                            hnswMValidator(1, maxHnswM()),
                                            hnswEfConstructionDefault(100),
                                            hnswEfConstructionValidator(1, maxHnswEfConstruction())))));
        }

        @Override
        public boolean acceptsValueInstanceType(Value candidate) {
            return candidate instanceof VectorCandidate;
        }
    };

    public static final SortedSet<VectorIndexVersion> KNOWN_VERSIONS;

    static {
        final TreeSet<VectorIndexVersion> versions = new TreeSet<>();
        for (final VectorIndexVersion version : values()) {
            if (version != UNKNOWN) {
                versions.add(version);
            }
        }
        KNOWN_VERSIONS = Collections.unmodifiableSortedSet(versions);
    }

    public static VectorIndexVersion latestSupportedVersion(KernelVersion kernelVersion) {
        for (final var version : KNOWN_VERSIONS.reversed()) {
            if (kernelVersion.isAtLeast(version.minimumRequiredKernelVersion)) {
                return version;
            }
        }
        return UNKNOWN;
    }

    public static VectorIndexVersion fromDescriptor(IndexProviderDescriptor descriptor) {
        for (final var version : KNOWN_VERSIONS.reversed()) {
            if (version.descriptor.equals(descriptor)) {
                return version;
            }
        }
        return UNKNOWN;
    }

    private static final KernelVersion LATEST_DEFAULT_KERNEL_VERSION =
            KernelVersion.getLatestVersion(Config.defaults());

    private final KernelVersion minimumRequiredKernelVersion;
    private final IndexProviderDescriptor descriptor;
    private final int maxDimensions;
    private final Map<String, VectorSimilarityFunction> similarityFunctions;
    private final Set<Boolean> quantizationBooleans;
    private final Set<VectorQuantizationType> quantizationTypes;
    private final int maxHnswM;
    private final int maxHnswEfConstruction;
    private final SortedMap<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> validators;
    private final TypedIndexSettingsValidator<VectorIndexConfig> defaultLatestIndexSettingValidator;

    VectorIndexVersion(
            IndexProviderDescriptor providerDescriptor,
            KernelVersion minimumRequiredKernelVersion,
            int maxDimensions,
            int maxHnswM,
            int maxHnswEfConstruction,
            Set<VectorSimilarityFunction> supportedSimilarityFunctions,
            Set<Boolean> supportedQuantizationEnableds,
            Set<VectorQuantizationType> supportedQuantizationTypes) {
        this.minimumRequiredKernelVersion = minimumRequiredKernelVersion;
        this.descriptor = providerDescriptor;
        this.maxDimensions = maxDimensions;
        {
            final Map<String, VectorSimilarityFunction> similarityFunctions = new TreeMap<>(CASE_INSENSITIVE_ORDER);
            for (final VectorSimilarityFunction similarityFunction : supportedSimilarityFunctions) {
                similarityFunctions.put(similarityFunction.functionName().toUpperCase(Locale.ROOT), similarityFunction);
            }
            this.similarityFunctions = Collections.unmodifiableMap(similarityFunctions);
        }
        this.quantizationBooleans = Collections.unmodifiableSortedSet(new TreeSet<>(supportedQuantizationEnableds));
        {
            final SortedSet<VectorQuantizationType> quantizationTypes =
                    new TreeSet<>(Comparator.comparing(Enum::name, CASE_INSENSITIVE_ORDER));
            quantizationTypes.addAll(supportedQuantizationTypes);
            this.quantizationTypes = Collections.unmodifiableSortedSet(quantizationTypes);
        }

        this.maxHnswM = maxHnswM;
        this.maxHnswEfConstruction = maxHnswEfConstruction;
        {
            final SortedMap<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> validators =
                    new TreeMap<>(Comparator.reverseOrder());
            validators.putAll(configureValidators());
            this.validators = Collections.unmodifiableSortedMap(validators);
        }
        this.defaultLatestIndexSettingValidator =
                findIndexSettingValidator(KernelVersion.getLatestVersion(Config.defaults()));
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

    protected abstract Map<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> configureValidators();

    public abstract boolean acceptsValueInstanceType(Value candidate);

    public VectorSimilarityFunction maybeSimilarityFunction(String name) {
        return similarityFunctions.get(name);
    }

    public VectorSimilarityFunction similarityFunction(String name) {
        final var similarityFunction = maybeSimilarityFunction(name);
        if (similarityFunction == null) {
            throw new IllegalArgumentException(
                    "'%s' is an unsupported vector similarity function for index with provider %s. "
                                    .formatted(name, descriptor.name())
                            + "Supported: "
                            + similarityFunctions.keySet());
        }

        return similarityFunction;
    }

    @VisibleForTesting
    public Collection<VectorSimilarityFunction> supportedSimilarityFunctions() {
        return similarityFunctions.values();
    }

    Map<String, VectorSimilarityFunction> nameToSimilarityFunction() {
        return similarityFunctions;
    }

    @VisibleForTesting
    public Set<Boolean> supportedQuantizationBooleans() {
        return quantizationBooleans;
    }

    @VisibleForTesting
    public Set<VectorQuantizationType> supportedQuantizationTypes() {
        return quantizationTypes;
    }

    public TypedIndexSettingsValidator<VectorIndexConfig> indexSettingValidator() {
        return indexSettingValidator(null);
    }

    /// Returns the latest validator that is compatible with the given `kernelVersion`.
    ///
    /// If the `kernelVersion` is `null`, the latest known version will be selected according to
    /// [KernelVersion#getLatestVersion(Configuration)] by passing the default configuration.
    ///
    /// If no validators is found for the given `kernelVersion`, the returned validator will throw an
    /// [InvalidArgumentException] for any method that is called on it.
    public TypedIndexSettingsValidator<VectorIndexConfig> indexSettingValidator(KernelVersion kernelVersion) {
        return kernelVersion != null ? findIndexSettingValidator(kernelVersion) : defaultLatestIndexSettingValidator;
    }

    private static class VersionedValidator extends TypedIndexSettingsValidator<VectorIndexConfig> {
        private final VectorIndexVersion version;

        VersionedValidator(
                VectorIndexVersion version,
                IndexSettingExtractors extractors,
                ValidatingIndexSettingsProcessor processor,
                IndexSettingEntry... injectedSettings) {
            super(version.descriptor(), new DefaultIndexSettingsValidator(extractors, processor, injectedSettings));
            this.version = version;
        }

        @Override
        protected VectorIndexConfig toTypedConfig(Iterable<Valid> records) {
            return new VectorIndexConfig(version, acceptedSettings(), records);
        }
    }

    private TypedIndexSettingsValidator<VectorIndexConfig> findIndexSettingValidator(KernelVersion kernelVersion) {
        for (Entry<KernelVersion, TypedIndexSettingsValidator<VectorIndexConfig>> entry : validators.entrySet()) {
            if (kernelVersion.isAtLeast(entry.getKey())) {
                return entry.getValue();
            }
        }
        return new NotFoundTypedIndexSettingsValidator<>(
                descriptor,
                InvalidArgumentException.internalError(
                        "Validator Not Found",
                        "Validator not found for '%s' on '%s'.".formatted(descriptor.name(), kernelVersion)));
    }
}
