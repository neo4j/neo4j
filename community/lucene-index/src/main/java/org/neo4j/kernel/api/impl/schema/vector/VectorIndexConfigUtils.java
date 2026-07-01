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
import static org.neo4j.internal.schema.IndexConfigUtils.INDEX_SETTING_COMPARATOR;
import static org.neo4j.internal.schema.SequencedIndexSettingProcessors.mergeToValidatingProcessor;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiPredicate;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexConfigUtils.IndexSettingsRequirement;
import org.neo4j.internal.schema.IndexSettingEntry;
import org.neo4j.internal.schema.IndexSettingExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.BooleanExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.DoubleExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.IntegerExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.StringExtractor;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsProcessor;
import org.neo4j.internal.schema.IndexSettingsProcessor.ValidatingIndexSettingsProcessor;
import org.neo4j.internal.schema.IndexSettingsRequirements.DefaultRequirement;
import org.neo4j.internal.schema.KnownIndexSettingRecords;
import org.neo4j.internal.schema.MissingDependentSettingMaterializer;
import org.neo4j.internal.schema.SingleIndexSettingConverter.IntegerToOptionalIntConverter;
import org.neo4j.internal.schema.SingleIndexSettingConverter.StringToUpperCaseConverter;
import org.neo4j.internal.schema.SingleIndexSettingConverter.TypeToOptionalConverter;
import org.neo4j.internal.schema.SingleIndexSettingLookup.NameToEnumLookup;
import org.neo4j.internal.schema.SingleIndexSettingLookup.SingleIndexSettingMapLookup;
import org.neo4j.internal.schema.SingleIndexSettingMigrator;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.FinalizePending;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.MissingSettingMaterializer;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.RemoveSetting;
import org.neo4j.internal.schema.SingleIndexSettingStorableNormalizer.EnumToNameStorableNormalizer;
import org.neo4j.internal.schema.SingleIndexSettingStorableNormalizer.SingleIndexSettingMapStorableNormalizer;
import org.neo4j.internal.schema.SingleIndexSettingValidator.DoubleRangeValidator;
import org.neo4j.internal.schema.SingleIndexSettingValidator.IntegerRangeValidator;
import org.neo4j.internal.schema.SingleIndexSettingValidator.OptionalIntRangeValidator;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class VectorIndexConfigUtils {
    static final IndexSetting DIMENSIONS = IndexSetting.vector_Dimensions();
    static final IndexSetting SIMILARITY_FUNCTION = IndexSetting.vector_Similarity_Function();
    static final IndexSetting DEFAULT_SEARCH_EXPANSION_FACTOR = IndexSetting.vector_Default_Search_Expansion_Factor();
    static final IndexSetting QUANTIZATION_ENABLED = IndexSetting.vector_Quantization_Enabled();
    static final IndexSetting QUANTIZATION_TYPE = IndexSetting.vector_Quantization_Type();
    static final IndexSetting HNSW_M = IndexSetting.vector_Hnsw_M();
    static final IndexSetting HNSW_EF_CONSTRUCTION = IndexSetting.vector_Hnsw_Ef_Construction();

    public static final SortedMap<IndexSetting, KernelVersion> INDEX_SETTING_INTRODUCED_VERSIONS;

    static {
        SortedMap<IndexSetting, KernelVersion> indexSettingIntroducedVersions = new TreeMap<>(INDEX_SETTING_COMPARATOR);
        indexSettingIntroducedVersions.put(DIMENSIONS, KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED);
        indexSettingIntroducedVersions.put(SIMILARITY_FUNCTION, KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED);
        indexSettingIntroducedVersions.put(
                DEFAULT_SEARCH_EXPANSION_FACTOR, KernelVersion.VERSION_VECTOR_BINARY_QUANTIZATION);
        indexSettingIntroducedVersions.put(
                QUANTIZATION_ENABLED, KernelVersion.VERSION_VECTOR_QUANTIZATION_AND_HYPER_PARAMS);
        indexSettingIntroducedVersions.put(QUANTIZATION_TYPE, KernelVersion.VERSION_VECTOR_BINARY_QUANTIZATION);
        indexSettingIntroducedVersions.put(HNSW_M, KernelVersion.VERSION_VECTOR_QUANTIZATION_AND_HYPER_PARAMS);
        indexSettingIntroducedVersions.put(
                HNSW_EF_CONSTRUCTION, KernelVersion.VERSION_VECTOR_QUANTIZATION_AND_HYPER_PARAMS);
        INDEX_SETTING_INTRODUCED_VERSIONS = Collections.unmodifiableSortedMap(indexSettingIntroducedVersions);
    }

    // ============
    //  dimensions
    // ============

    static final IndexSettingExtractor DIMENSIONS_EXTRACTOR = IntegerExtractor.of(DIMENSIONS);

    static ValidatingIndexSettingsProcessor dimensionValidator(int min, int max) {
        return IntegerRangeValidator.of(DIMENSIONS, min, max);
    }

    static IndexSettingsProcessor optionalDimensionDefault(OptionalInt dimensions) {
        return MissingSettingMaterializer.of(DIMENSIONS, dimensions, dimensions, Values.NO_VALUE);
    }

    static final IndexSettingsProcessor OPTIONAL_DIMENSION_CONVERTER = IntegerToOptionalIntConverter.of(DIMENSIONS);

    static ValidatingIndexSettingsProcessor optionalDimensionValidator(int min, int max) {
        return OptionalIntRangeValidator.of(DIMENSIONS, min, max);
    }

    // =====================
    //  similarity function
    // =====================

    static final IndexSettingExtractor SIMILARITY_FUNCTION_EXTRACTOR = StringExtractor.of(SIMILARITY_FUNCTION);

    static final IndexSettingsProcessor SIMILARITY_FUNCTION_UPPER_CASE_CONVERTER =
            StringToUpperCaseConverter.of(SIMILARITY_FUNCTION);

    static IndexSettingsProcessor similarityFunctionDefault(VectorSimilarityFunction similarityFunction) {
        return MissingSettingMaterializer.forVerification(SIMILARITY_FUNCTION, similarityFunction.functionName());
    }

    static ValidatingIndexSettingsProcessor similarityFunctionLookup(VectorSimilarityFunction... similarityFunctions) {
        Map<String, VectorSimilarityFunction> lookup = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        for (VectorSimilarityFunction similarityFunction : similarityFunctions) {
            String name = similarityFunction.functionName().toUpperCase(Locale.ROOT);
            VectorSimilarityFunction existingSimilarityFunction = lookup.put(name, similarityFunction);
            throw new IllegalArgumentException(
                    "Expected a single %s to be provided for '%s', multiple given. Provided both `%s` and `%s`."
                            .formatted(
                                    VectorSimilarityFunction.class.getSimpleName(),
                                    name,
                                    existingSimilarityFunction,
                                    similarityFunction));
        }
        return similarityFunctionLookup(lookup);
    }

    static ValidatingIndexSettingsProcessor similarityFunctionLookup(Map<String, VectorSimilarityFunction> lookup) {
        return SingleIndexSettingMapLookup.of(SIMILARITY_FUNCTION, String.class, lookup);
    }

    static IndexSettingsProcessor similarityFunctionNormalizer(Map<String, VectorSimilarityFunction> lookup) {
        Map<VectorSimilarityFunction, TextValue> inverted = new HashMap<>(lookup.size());
        for (Entry<String, VectorSimilarityFunction> entry : lookup.entrySet()) {
            inverted.put(entry.getValue(), Values.utf8Value(entry.getKey()));
        }
        return SingleIndexSettingMapStorableNormalizer.of(
                SIMILARITY_FUNCTION, VectorSimilarityFunction.class, inverted);
    }

    // =================================
    //  default search expansion factor
    // =================================

    static final IndexSettingExtractor DEFAULT_SEARCH_EXPANSION_FACTOR_EXTRACTOR =
            DoubleExtractor.of(DEFAULT_SEARCH_EXPANSION_FACTOR);

    static IndexSettingEntry defaultSearchExpansionFactor(double expansionFactor) {
        return new IndexSettingEntry(
                DEFAULT_SEARCH_EXPANSION_FACTOR, expansionFactor, Values.doubleValue(expansionFactor));
    }

    @SafeVarargs
    static IndexSettingsProcessor defaultSearchExpansionFactorDefault(
            Entry<VectorQuantizationType, Double>... defaultsForVerification) {
        return MissingDefaultSearchExpansionFactorMaterializer.of(Map.ofEntries(defaultsForVerification));
    }

    static final class MissingDefaultSearchExpansionFactorMaterializer
            extends MissingDependentSettingMaterializer<VectorQuantizationType, Double> {
        private final Map<VectorQuantizationType, Double> dependentValueLookupForVerification;

        static MissingDefaultSearchExpansionFactorMaterializer of(
                Map<VectorQuantizationType, Double> defaultLookupForVerification) {
            return new MissingDefaultSearchExpansionFactorMaterializer(defaultLookupForVerification);
        }

        private MissingDefaultSearchExpansionFactorMaterializer(
                Map<VectorQuantizationType, Double> dependentValueLookupForVerification) {
            super(QUANTIZATION_TYPE, DEFAULT_SEARCH_EXPANSION_FACTOR, double.class);
            this.dependentValueLookupForVerification = dependentValueLookupForVerification;
        }

        @Override
        protected Double dependentValueForVerification(VectorQuantizationType dependency) {
            return dependentValueLookupForVerification.get(dependency);
        }
    }

    static ValidatingIndexSettingsProcessor defaultSearchExpansionFactorValidator(double min, double max) {
        return DoubleRangeValidator.of(DEFAULT_SEARCH_EXPANSION_FACTOR, min, max);
    }

    // ======================
    //  quantization enabled
    // ======================

    static final IndexSettingExtractor QUANTIZATION_ENABLED_EXTRACTOR = BooleanExtractor.of(QUANTIZATION_ENABLED);

    static IndexSettingsProcessor quantizationEnabledDefault(
            boolean valueForAuthoritativeRead, boolean valueForVerification) {
        return MissingSettingMaterializer.of(
                QUANTIZATION_ENABLED,
                valueForAuthoritativeRead,
                valueForVerification,
                Values.booleanValue(valueForVerification));
    }

    static IndexSettingsProcessor quantizationEnabledDefault(boolean quantizationEnabled) {
        return MissingSettingMaterializer.forVerification(
                QUANTIZATION_ENABLED, quantizationEnabled, Values.booleanValue(quantizationEnabled));
    }

    static final ValidatingIndexSettingsProcessor QUANTIZATION_ENABLED_VALIDATOR =
            FinalizePending.of(QUANTIZATION_ENABLED);

    static final IndexSettingsProcessor OPTIONAL_QUANTIZATION_ENABLED_CONVERTER =
            TypeToOptionalConverter.of(QUANTIZATION_ENABLED, Boolean.class);

    static IndexSettingsProcessor optionalQuantizationEnabledDefault(Optional<Boolean> quantizationEnabled) {
        return MissingSettingMaterializer.of(
                QUANTIZATION_ENABLED, quantizationEnabled, quantizationEnabled, Values.NO_VALUE);
    }

    static ValidatingIndexSettingsProcessor quantizationEnabledToTypeMigrator(
            VectorQuantizationType correspondingEnabledType) {
        return SimpleQuantizationEnabledToTypeMigrator.of(correspondingEnabledType);
    }

    static final class SimpleQuantizationEnabledToTypeMigrator
            extends SingleIndexSettingMigrator<Boolean, VectorQuantizationType>
            implements ValidatingIndexSettingsProcessor {
        private final VectorQuantizationType correspondingEnabledType;

        static SimpleQuantizationEnabledToTypeMigrator of(VectorQuantizationType correspondingEnabledType) {
            return new SimpleQuantizationEnabledToTypeMigrator(correspondingEnabledType);
        }

        private SimpleQuantizationEnabledToTypeMigrator(VectorQuantizationType correspondingEnabledType) {
            super(QUANTIZATION_ENABLED, Boolean.class, QUANTIZATION_TYPE, VectorQuantizationType.class);
            this.correspondingEnabledType = correspondingEnabledType;
        }

        @Override
        protected VectorQuantizationType migrate(Boolean value) {
            return value ? correspondingEnabledType : VectorQuantizationType.NONE;
        }

        @Override
        protected Value toStorable(VectorQuantizationType value) {
            return Values.utf8Value(value.name());
        }

        @Override
        public RecordWithSetting processForVerification(RecordWithSetting record) {
            RecordWithSetting migratedRecord = super.processForVerification(record);
            return switch (migratedRecord) {
                case Pending pending when pending.setting().equals(toSetting) -> new Valid(pending);
                default -> migratedRecord;
            };
        }
    }

    static final IndexSettingsProcessor REMOVE_QUANTIZATION_ENABLED = RemoveSetting.of(QUANTIZATION_ENABLED);

    // ===================
    //  quantization type
    // ===================

    static final IndexSettingExtractor QUANTIZATION_TYPE_EXTRACTOR = StringExtractor.of(QUANTIZATION_TYPE);

    static final IndexSettingsProcessor QUANTIZATION_TYPE_UPPER_CASE_CONVERTER =
            StringToUpperCaseConverter.of(QUANTIZATION_TYPE);

    static IndexSettingsProcessor quantizationTypeDefault(VectorQuantizationType quantizationType) {
        return MissingQuantizationTypeMaterializer.of(quantizationType);
    }

    static final class MissingQuantizationTypeMaterializer
            extends MissingDependentSettingMaterializer<Optional<Boolean>, String> {
        private final VectorQuantizationType dependentValueForVerification;

        static MissingQuantizationTypeMaterializer of(VectorQuantizationType dependentValueForVerification) {
            return new MissingQuantizationTypeMaterializer(dependentValueForVerification);
        }

        private MissingQuantizationTypeMaterializer(VectorQuantizationType dependentValueForVerification) {
            super(QUANTIZATION_ENABLED, QUANTIZATION_TYPE, String.class);
            this.dependentValueForVerification = dependentValueForVerification;
        }

        @Override
        protected String dependentValueForVerification(Optional<Boolean> dependency) {
            VectorQuantizationType type =
                    dependency.orElse(true) ? dependentValueForVerification : VectorQuantizationType.NONE;
            return type.name();
        }
    }

    static ValidatingIndexSettingsProcessor quantizationTypeLookup(
            VectorQuantizationType first, VectorQuantizationType... rest) {
        return QuantizationTypeLookup.of(first, rest);
    }

    static ValidatingIndexSettingsProcessor quantizationTypeLookup(Set<VectorQuantizationType> quantizationTypes) {
        return QuantizationTypeLookup.of(quantizationTypes);
    }

    static final class QuantizationTypeLookup implements ValidatingIndexSettingsProcessor {
        static final BiPredicate<Optional<Boolean>, VectorQuantizationType> JOINT_VALUE_VALIDATOR =
                (optionalEnabled, type) -> {
                    if (optionalEnabled.isEmpty()) {
                        return true;
                    }
                    boolean enabled = optionalEnabled.get();
                    return !enabled && type == VectorQuantizationType.NONE
                            || enabled && type != VectorQuantizationType.NONE;
                };

        private final ValidatingIndexSettingsProcessor independentSettingsValidator;
        private final IndexSettingsRequirement<?> requirement;

        static ValidatingIndexSettingsProcessor of(VectorQuantizationType first, VectorQuantizationType... rest) {
            return of(EnumSet.of(first, rest));
        }

        static ValidatingIndexSettingsProcessor of(Set<VectorQuantizationType> quantizationTypes) {
            return new QuantizationTypeLookup(quantizationTypes);
        }

        private QuantizationTypeLookup(Set<VectorQuantizationType> quantizationTypes) {
            this.independentSettingsValidator = mergeToValidatingProcessor(
                    FinalizePending.of(QUANTIZATION_ENABLED),
                    NameToEnumLookup.of(QUANTIZATION_TYPE, quantizationTypes));

            // build up supported message
            Optional<Boolean> optEmpty = Optional.empty();
            Optional<Boolean> optFalse = Optional.of(false);
            Optional<Boolean> optTrue = Optional.of(true);
            StringBuilder sb = new StringBuilder()
                    .append("('")
                    .append(QUANTIZATION_ENABLED.getSettingName())
                    .append("', '")
                    .append(QUANTIZATION_TYPE.getSettingName())
                    .append("') ::");

            // false, NONE
            sb.append(" (")
                    .append(optFalse)
                    .append(", ")
                    .append(VectorQuantizationType.NONE)
                    .append(")");

            // true, not NONE
            for (VectorQuantizationType type : quantizationTypes) {
                if (type != VectorQuantizationType.NONE) {
                    sb.append(" | (").append(optTrue).append(", ").append(type).append(")");
                }
            }

            // NULL, _
            for (VectorQuantizationType type : quantizationTypes) {
                sb.append(" | (").append(optEmpty).append(", ").append(type).append(")");
            }

            String supported = sb.toString();
            this.requirement = new DefaultRequirement<>(JOINT_VALUE_VALIDATOR) {
                @Override
                public String supported() {
                    return supported;
                }
            };
        }

        @Override
        public void updateForVerification(KnownIndexSettingRecords records) {
            independentSettingsValidator.updateForVerification(records);
            RecordWithSetting enabledRecord = records.get(QUANTIZATION_ENABLED);
            RecordWithSetting typeRecord = records.get(QUANTIZATION_TYPE);
            if (enabledRecord instanceof Valid validEnabled
                    && typeRecord instanceof Valid validType
                    && !JOINT_VALUE_VALIDATOR.test(validEnabled.get(), validType.get())) {
                // incompatable settings
                records.upsert(new InvalidValue(validEnabled, requirement));
                records.upsert(new InvalidValue(validType, requirement));
            }
        }

        @Override
        public void updateForAuthoritativeRead(KnownIndexSettingRecords records) {
            independentSettingsValidator.updateForAuthoritativeRead(records);
        }

        @Override
        public Set<IndexSetting> settings() {
            return independentSettingsValidator.settings();
        }

        @Override
        public String toString() {
            return Iterables.toString(settings(), ", ", getClass().getSimpleName() + "[", "]");
        }
    }

    static IndexSettingsProcessor quantizationTypeNormalizer(Set<VectorQuantizationType> quantizationTypes) {
        return EnumToNameStorableNormalizer.of(QUANTIZATION_TYPE, VectorQuantizationType.class, quantizationTypes);
    }

    static IndexSettingEntry quantizationType(VectorQuantizationType quantizationType) {
        return new IndexSettingEntry(QUANTIZATION_TYPE, quantizationType, Values.utf8Value(quantizationType.name()));
    }

    // ========
    //  hnsw m
    // ========

    static final IndexSettingExtractor HNSW_M_EXTRACTOR = IntegerExtractor.of(HNSW_M);

    static IndexSettingsProcessor hnswMDefault(int m) {
        return MissingSettingMaterializer.of(VectorIndexConfigUtils.HNSW_M, m, m, Values.intValue(m));
    }

    static ValidatingIndexSettingsProcessor hnswMValidator(int min, int max) {
        return IntegerRangeValidator.of(HNSW_M, min, max);
    }

    static IndexSettingEntry hnswM(int M) {
        return new IndexSettingEntry(HNSW_M, M, Values.intValue(M));
    }

    // ======================
    //  hnsw ef construction
    // ======================

    static final IndexSettingExtractor HNSW_EF_CONSTRUCTION_EXTRACTOR = IntegerExtractor.of(HNSW_EF_CONSTRUCTION);

    static IndexSettingsProcessor hnswEfConstructionDefault(int efConstruction) {
        return MissingSettingMaterializer.of(
                VectorIndexConfigUtils.HNSW_EF_CONSTRUCTION,
                efConstruction,
                efConstruction,
                Values.intValue(efConstruction));
    }

    static ValidatingIndexSettingsProcessor hnswEfConstructionValidator(int min, int max) {
        return IntegerRangeValidator.of(HNSW_EF_CONSTRUCTION, min, max);
    }

    static IndexSettingEntry hnswEfConstruction(int efConstruction) {
        return new IndexSettingEntry(HNSW_EF_CONSTRUCTION, efConstruction, Values.intValue(efConstruction));
    }
}
