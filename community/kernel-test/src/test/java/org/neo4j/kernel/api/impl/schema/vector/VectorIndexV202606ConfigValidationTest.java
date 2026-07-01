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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.iterable;
import static org.assertj.core.groups.Tuple.tuple;
import static org.neo4j.internal.schema.IndexSettingRecord.State.INVALID_VALUE;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.DEFAULT_SEARCH_EXPANSION_FACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.DIMENSIONS;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_EF_CONSTRUCTION;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_M;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_ENABLED;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_TYPE;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.SIMILARITY_FUNCTION;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.assertAndReturnFunctionDoesNotThrow;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.assertIncorrectType;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.assertInvalidValue;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.assertUnrecognizedSetting;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.assertVectorIndexConfigSetting;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.similarityFunctionsToString;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.validateAsInvalid;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.validateAsValid;

import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexConfigUtils.HasSetting;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.State;
import org.neo4j.internal.schema.IndexSettingRecordsByState;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.TypedIndexSettingsValidator;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig.HnswConfig;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.test.LatestVersions;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

class VectorIndexV202606ConfigValidationTest {
    private static final VectorIndexVersion VERSION = VectorIndexVersion.V2026_06;
    private static final TypedIndexSettingsValidator<VectorIndexConfig> VALIDATOR =
            VERSION.indexSettingValidator(LatestVersions.LATEST_KERNEL_VERSION);

    @Test
    void validIndexConfig() {
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withHnswM(16)
                .withHnswEfConstruction(100)
                .withQuantizationType(VectorQuantizationType.BINARY)
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withDefaultSearchExpansionFactor(16.0)
                .toSettingsAccessor();

        validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig =
                assertAndReturnFunctionDoesNotThrow(() -> VALIDATOR.validateToTypedConfig(settings));

        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::dimensions,
                        VectorIndexConfig::similarityFunction,
                        VectorIndexConfig::defaultSearchExpansionFactor,
                        VectorIndexConfig::quantization,
                        VectorIndexConfig::hnsw)
                .containsExactly(
                        OptionalInt.of(VERSION.maxDimensions()),
                        VERSION.similarityFunction("COSINE"),
                        16.0,
                        VectorQuantizationType.BINARY,
                        new HnswConfig(16, 100));

        assertThat(vectorIndexConfig.config().settingNames())
                .containsExactlyInAnyOrder(
                        DIMENSIONS.getSettingName(),
                        SIMILARITY_FUNCTION.getSettingName(),
                        DEFAULT_SEARCH_EXPANSION_FACTOR.getSettingName(),
                        QUANTIZATION_TYPE.getSettingName(),
                        HNSW_M.getSettingName(),
                        HNSW_EF_CONSTRUCTION.getSettingName());
    }

    @Test
    void validIndexConfigWithDefaults() {
        SettingsAccessor settings = VectorIndexSettings.create().toSettingsAccessor();

        validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig =
                assertAndReturnFunctionDoesNotThrow(() -> VALIDATOR.validateToTypedConfig(settings));

        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::dimensions,
                        VectorIndexConfig::similarityFunction,
                        VectorIndexConfig::defaultSearchExpansionFactor,
                        VectorIndexConfig::quantization,
                        VectorIndexConfig::hnsw)
                .containsExactly(
                        OptionalInt.empty(),
                        VERSION.similarityFunction("COSINE"),
                        1.5,
                        VectorQuantizationType.SCALAR,
                        new HnswConfig(16, 100));

        assertThat(vectorIndexConfig.config().settingNames())
                .containsExactlyInAnyOrder(
                        SIMILARITY_FUNCTION.getSettingName(),
                        DEFAULT_SEARCH_EXPANSION_FACTOR.getSettingName(),
                        QUANTIZATION_TYPE.getSettingName(),
                        HNSW_M.getSettingName(),
                        HNSW_EF_CONSTRUCTION.getSettingName());
    }

    @Test
    void unrecognisedSetting() {
        IndexSetting unrecognisedSetting = IndexSetting.fulltext_Analyzer();
        SettingsAccessor settings =
                VectorIndexSettings.create().set(unrecognisedSetting, "swedish").toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertUnrecognizedSetting(validationRecords, unrecognisedSetting.getSettingName());
        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        "Invalid index config key 'fulltext.analyzer', it was not recognized as an index setting.");
    }

    @Test
    void incorrectTypeForDimensions() {
        String incorrectDimensions = String.valueOf(VERSION.maxDimensions());
        SettingsAccessor settings = VectorIndexSettings.create()
                .set(DIMENSIONS, incorrectDimensions)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertIncorrectType(
                validationRecords,
                DIMENSIONS,
                Values.stringValue(incorrectDimensions),
                TextValue.class,
                IntegralValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Wrong type for vector.dimensions. Expected INTEGER, got STRING");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void nonPositiveDimensions(int invalidDimensions) {
        SettingsAccessor settings =
                VectorIndexSettings.create().withDimensions(invalidDimensions).toSettingsAccessor();

        assertInvalidDimensions(invalidDimensions, settings);
    }

    @Test
    void aboveMaxDimensions() {
        int invalidDimensions = VERSION.maxDimensions() + 1;
        SettingsAccessor settings =
                VectorIndexSettings.create().withDimensions(invalidDimensions).toSettingsAccessor();

        assertInvalidDimensions(invalidDimensions, settings);
    }

    private static void assertInvalidDimensions(int invalidDimensions, SettingsAccessor settings) {
        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertInvalidValue(validationRecords, DIMENSIONS, OptionalInt.of(invalidDimensions));
        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        DIMENSIONS.getSettingName(), "must be between 1 and", String.valueOf(VERSION.maxDimensions()));
    }

    @Test
    void incorrectTypeForSimilarityFunction() {
        long incorrectSimilarityFunction = 123L;
        SettingsAccessor settings = VectorIndexSettings.create()
                .set(SIMILARITY_FUNCTION, incorrectSimilarityFunction)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertIncorrectType(
                validationRecords,
                SIMILARITY_FUNCTION,
                Values.longValue(incorrectSimilarityFunction),
                NumberValue.class,
                TextValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll("Wrong type for vector.similarity_function. Expected STRING, got INTEGER");
    }

    @Test
    void invalidSimilarityFunction() {
        String invalidSimilarityFunction = "ClearlyThisIsNotASimilarityFunction";
        SettingsAccessor settings = VectorIndexSettings.create()
                .set(IndexSetting.vector_Similarity_Function(), invalidSimilarityFunction)
                .toSettingsAccessor();
        String normalizedInvalidSimilarityFunction = invalidSimilarityFunction.toUpperCase(Locale.ROOT);

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertInvalidValue(validationRecords, SIMILARITY_FUNCTION, normalizedInvalidSimilarityFunction);

        String supportedSimilarityFunctions = similarityFunctionsToString(VERSION.supportedSimilarityFunctions());
        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        normalizedInvalidSimilarityFunction,
                        "is an unsupported",
                        SIMILARITY_FUNCTION.getSettingName(),
                        supportedSimilarityFunctions);
    }

    @Test
    void nonUpperCaseSimilarityFunction() {
        String mixedCaseSimilarityFunction = "coSIne";
        SettingsAccessor settings = VectorIndexSettings.create()
                .withSimilarityFunction(mixedCaseSimilarityFunction)
                .toSettingsAccessor();
        VectorSimilarityFunction corespondingSimilarityFunction = VERSION.similarityFunction("COSINE");

        IndexSettingRecordsByState validationRecords = validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig = VALIDATOR.validateToTypedConfig(validationRecords);
        assertVectorIndexConfigSetting(
                vectorIndexConfig,
                SIMILARITY_FUNCTION,
                VectorIndexConfig::similarityFunction,
                corespondingSimilarityFunction,
                Values.utf8Value(corespondingSimilarityFunction.functionName()));
    }

    @Test
    void incorrectTypeForDefaultSearchExpansionFactor() {
        String incorrectExpansionFactor = "10";
        SettingsAccessor settings = VectorIndexSettings.create()
                .set(DEFAULT_SEARCH_EXPANSION_FACTOR, incorrectExpansionFactor)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertIncorrectType(
                validationRecords,
                DEFAULT_SEARCH_EXPANSION_FACTOR,
                Values.utf8Value(incorrectExpansionFactor),
                TextValue.class,
                NumberValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage(
                        "Wrong type for vector.default_search_expansion_factor. Expected INTEGER|FLOAT, got STRING");
    }

    @ParameterizedTest
    @ValueSource(doubles = {-1.0, 0.0, 0.5})
    void lessThanOneDefaultSearchExpansionFactor(double expansionFactor) {
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDefaultSearchExpansionFactor(expansionFactor)
                .toSettingsAccessor();

        assertSingleInvalidDefaultSearchExpansionFactor(expansionFactor, settings);
    }

    @Test
    void aboveMaxDefaultSearchExpansionFactor() {
        double invalidExpansionFactor = Math.nextUp(Double.MAX_VALUE);
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDefaultSearchExpansionFactor(invalidExpansionFactor)
                .toSettingsAccessor();

        assertSingleInvalidDefaultSearchExpansionFactor(invalidExpansionFactor, settings);
    }

    @ParameterizedTest
    @ValueSource(
            doubles = {
                Double.NEGATIVE_INFINITY,
                Float.NEGATIVE_INFINITY,
                Float.NaN,
                Double.NaN,
                Float.POSITIVE_INFINITY,
                Double.POSITIVE_INFINITY
            })
    void nonFiniteDefaultSearchExpansionFactor(double expansionFactor) {
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDefaultSearchExpansionFactor(expansionFactor)
                .toSettingsAccessor();

        assertSingleInvalidDefaultSearchExpansionFactor(expansionFactor, settings);
    }

    private static void assertSingleInvalidDefaultSearchExpansionFactor(
            Double expansionFactor, SettingsAccessor settings) {
        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertInvalidValue(validationRecords, DEFAULT_SEARCH_EXPANSION_FACTOR, expansionFactor);
        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        DEFAULT_SEARCH_EXPANSION_FACTOR.getSettingName(),
                        "must be between 1.0 and",
                        String.valueOf(10_000.0));
    }

    @Test
    void invalidDefaultSearchExpansionFactorDueToQuantizationType() {
        String incorrectQuantizationType = "ClearlyThisIsNotAQuantizationType";
        SettingsAccessor settings = VectorIndexSettings.create()
                .set(QUANTIZATION_TYPE, incorrectQuantizationType)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertThat(validationRecords.get(State.INVALID_VALUE))
                .hasOnlyElementsOfType(RecordWithSetting.class)
                .extracting(InvalidValue.class::cast)
                .filteredOn(InvalidValue::setting, DEFAULT_SEARCH_EXPANSION_FACTOR)
                .singleElement()
                .extracting(HasSetting::setting, RecordWithValue::value)
                .containsExactly(DEFAULT_SEARCH_EXPANSION_FACTOR, null);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        "is an unsupported",
                        DEFAULT_SEARCH_EXPANSION_FACTOR.getSettingName(),
                        "Supported",
                        "non-null instance of double");
    }

    @Test
    void incorrectTypeForQuantizationEnabled() {
        long incorrectQuantizationEnabled = 123L;
        SettingsAccessor settings = VectorIndexSettings.create()
                .set(QUANTIZATION_ENABLED, incorrectQuantizationEnabled)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertIncorrectType(
                validationRecords,
                QUANTIZATION_ENABLED,
                Values.longValue(incorrectQuantizationEnabled),
                NumberValue.class,
                BooleanValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Wrong type for vector.quantization.enabled. Expected BOOLEAN, got INTEGER");
    }

    @Test
    void incorrectTypeForQuantizationType() {
        long incorrectQuantizationType = 123L;
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDefaultSearchExpansionFactor(2.0)
                .set(QUANTIZATION_TYPE, incorrectQuantizationType)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertIncorrectType(
                validationRecords,
                QUANTIZATION_TYPE,
                Values.longValue(incorrectQuantizationType),
                NumberValue.class,
                TextValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll("Wrong type for vector.quantization.type. Expected STRING, got INTEGER");
    }

    @Test
    void invalidQuantizationType() {
        String incorrectQuantizationType = "ClearlyThisIsNotAQuantizationType";
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDefaultSearchExpansionFactor(2.0)
                .set(QUANTIZATION_TYPE, incorrectQuantizationType)
                .toSettingsAccessor();
        String normalizedIncorrectQuantizationType = incorrectQuantizationType.toUpperCase(Locale.ROOT);

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertInvalidValue(validationRecords, QUANTIZATION_TYPE, normalizedIncorrectQuantizationType);

        String supportedQuantizationTypes = Iterables.toString(VERSION.supportedQuantizationTypes(), ", ", "[", "]");
        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        normalizedIncorrectQuantizationType,
                        "is an unsupported",
                        QUANTIZATION_TYPE.getSettingName(),
                        supportedQuantizationTypes);
    }

    @Test
    void nonUpperCaseQuantizationType() {
        String mixedCaseQuantizationType = "sCAlaR";
        SettingsAccessor settings = VectorIndexSettings.create()
                .withQuantizationType(mixedCaseQuantizationType)
                .toSettingsAccessor();
        final VectorQuantizationType corespondingQuantizationType = VectorQuantizationType.SCALAR;

        IndexSettingRecordsByState validationRecords = validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig = VALIDATOR.validateToTypedConfig(validationRecords);
        assertVectorIndexConfigSetting(
                vectorIndexConfig,
                QUANTIZATION_TYPE,
                VectorIndexConfig::quantization,
                corespondingQuantizationType,
                Values.utf8Value(corespondingQuantizationType.name()));
    }

    @Test
    void invalidQuantizationEnabledFalseForQuantizationType() {
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDefaultSearchExpansionFactor(2.0)
                .withQuantizationDisabled()
                .withQuantizationType(VectorQuantizationType.SCALAR)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertThat(validationRecords.get(INVALID_VALUE))
                .hasSize(2)
                .hasOnlyElementsOfType(InvalidValue.class)
                .extracting(InvalidValue.class::cast)
                .asInstanceOf(iterable(InvalidValue.class))
                .extracting(HasSetting::setting, RecordWithValue::value)
                .containsExactlyInAnyOrder(
                        tuple(QUANTIZATION_ENABLED, Optional.of(false)),
                        tuple(QUANTIZATION_TYPE, VectorQuantizationType.SCALAR));

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .message()
                .contains("is an unsupported")
                .containsAnyOf(QUANTIZATION_ENABLED.getSettingName(), QUANTIZATION_TYPE.getSettingName());
    }

    @Test
    void invalidQuantizationEnabledTrueForQuantizationType() {
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDefaultSearchExpansionFactor(2.0)
                .withQuantizationEnabled()
                .withQuantizationType(VectorQuantizationType.NONE)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertThat(validationRecords.get(INVALID_VALUE))
                .hasSize(2)
                .hasOnlyElementsOfType(InvalidValue.class)
                .extracting(InvalidValue.class::cast)
                .asInstanceOf(iterable(InvalidValue.class))
                .extracting(HasSetting::setting, RecordWithValue::value)
                .containsExactlyInAnyOrder(
                        tuple(QUANTIZATION_ENABLED, Optional.of(true)),
                        tuple(QUANTIZATION_TYPE, VectorQuantizationType.NONE));

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .message()
                .contains("is an unsupported")
                .containsAnyOf(QUANTIZATION_ENABLED.getSettingName(), QUANTIZATION_TYPE.getSettingName());
    }

    @Test
    void validDefaultQuantization() {
        final VectorQuantizationType defaultQuantizationType = VectorQuantizationType.SCALAR;
        SettingsAccessor settings = VectorIndexSettings.create().toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig = VALIDATOR.validateToTypedConfig(validationRecords);
        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::quantizationEnabled,
                        config -> config.get(QUANTIZATION_ENABLED),
                        config -> config.getValue(QUANTIZATION_ENABLED))
                .containsExactly(true, null, Values.NO_VALUE);

        assertVectorIndexConfigSetting(
                vectorIndexConfig,
                QUANTIZATION_TYPE,
                VectorIndexConfig::quantization,
                defaultQuantizationType,
                Values.utf8Value(defaultQuantizationType.name()));
    }

    @Test
    void validQuantizationEnabledFalseNoQuantizationType() {
        final VectorQuantizationType defaultQuantizationType = VectorQuantizationType.NONE;
        SettingsAccessor settings =
                VectorIndexSettings.create().withQuantizationDisabled().toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig = VALIDATOR.validateToTypedConfig(validationRecords);
        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::quantizationEnabled,
                        config -> config.get(QUANTIZATION_ENABLED),
                        config -> config.getValue(QUANTIZATION_ENABLED))
                .containsExactly(false, null, Values.NO_VALUE);

        assertVectorIndexConfigSetting(
                vectorIndexConfig,
                QUANTIZATION_TYPE,
                VectorIndexConfig::quantization,
                defaultQuantizationType,
                Values.utf8Value(defaultQuantizationType.name()));
    }

    @Test
    void validQuantizationEnabledTrueNoQuantizationType() {
        final VectorQuantizationType defaultQuantizationType = VectorQuantizationType.SCALAR;
        SettingsAccessor settings =
                VectorIndexSettings.create().withQuantizationEnabled().toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig = VALIDATOR.validateToTypedConfig(validationRecords);
        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::quantizationEnabled,
                        config -> config.get(QUANTIZATION_ENABLED),
                        config -> config.getValue(QUANTIZATION_ENABLED))
                .containsExactly(true, null, Values.NO_VALUE);

        assertVectorIndexConfigSetting(
                vectorIndexConfig,
                QUANTIZATION_TYPE,
                VectorIndexConfig::quantization,
                defaultQuantizationType,
                Values.utf8Value(defaultQuantizationType.name()));
    }

    @Test
    void validQuantizationEnabledFalseForQuantizationType() {
        final VectorQuantizationType quantizationType = VectorQuantizationType.NONE;
        SettingsAccessor settings = VectorIndexSettings.create()
                .withQuantizationDisabled()
                .withQuantizationType(quantizationType)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig = VALIDATOR.validateToTypedConfig(validationRecords);
        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::quantizationEnabled,
                        config -> config.get(QUANTIZATION_ENABLED),
                        config -> config.getValue(QUANTIZATION_ENABLED))
                .containsExactly(false, null, Values.NO_VALUE);

        assertVectorIndexConfigSetting(
                vectorIndexConfig,
                QUANTIZATION_TYPE,
                VectorIndexConfig::quantization,
                quantizationType,
                Values.utf8Value(quantizationType.name()));
    }

    @Test
    void validQuantizationEnabledTrueForQuantizationType() {
        final VectorQuantizationType quantizationType = VectorQuantizationType.SCALAR;
        SettingsAccessor settings = VectorIndexSettings.create()
                .withQuantizationEnabled()
                .withQuantizationType(quantizationType)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsValid(VALIDATOR, settings);
        VectorIndexConfig vectorIndexConfig = VALIDATOR.validateToTypedConfig(validationRecords);
        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::quantizationEnabled,
                        config -> config.get(QUANTIZATION_ENABLED),
                        config -> config.getValue(QUANTIZATION_ENABLED))
                .containsExactly(true, null, Values.NO_VALUE);

        assertVectorIndexConfigSetting(
                vectorIndexConfig,
                QUANTIZATION_TYPE,
                VectorIndexConfig::quantization,
                quantizationType,
                Values.utf8Value(quantizationType.name()));
    }

    @Test
    void incorrectTypeForHnswM() {
        String incorrectHnswM = "Here is a String";
        SettingsAccessor settings =
                VectorIndexSettings.create().set(HNSW_M, incorrectHnswM).toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertIncorrectType(
                validationRecords, HNSW_M, Values.stringValue(incorrectHnswM), TextValue.class, IntegralValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Wrong type for vector.hnsw.m. Expected INTEGER, got STRING");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void nonPositiveHnswM(int invalidM) {
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withHnswM(invalidM)
                .toSettingsAccessor();

        assertInvalidM(invalidM, settings);
    }

    @Test
    void aboveMaxHnswM() {
        int invalidHnswM = VERSION.maxHnswM() + 1;
        SettingsAccessor settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withHnswM(invalidHnswM)
                .toSettingsAccessor();

        assertInvalidM(invalidHnswM, settings);
    }

    private static void assertInvalidM(int invalidM, SettingsAccessor settings) {
        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertInvalidValue(validationRecords, HNSW_M, invalidM);
        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        HNSW_M.getSettingName(), "must be between 1 and", String.valueOf(VERSION.maxHnswM()));
    }

    @Test
    void incorrectTypeForHnswEfConstruction() {
        String incorrectHnswEfConstruction = "Here is a String";
        SettingsAccessor settings = VectorIndexSettings.create()
                .set(HNSW_EF_CONSTRUCTION, incorrectHnswEfConstruction)
                .toSettingsAccessor();

        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertIncorrectType(
                validationRecords,
                HNSW_EF_CONSTRUCTION,
                Values.stringValue(incorrectHnswEfConstruction),
                TextValue.class,
                IntegralValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Wrong type for vector.hnsw.ef_construction. Expected INTEGER, got STRING");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void nonPositiveHnswEfConstruction(int invalidEfConstruction) {
        SettingsAccessor settings = VectorIndexSettings.create()
                .withHnswEfConstruction(invalidEfConstruction)
                .toSettingsAccessor();

        assertInvalidEfConstruction(invalidEfConstruction, settings);
    }

    @Test
    void aboveMaxHnswEfConstruction() {
        int invalidHnswEfConstruction = VERSION.maxHnswEfConstruction() + 1;
        SettingsAccessor settings = VectorIndexSettings.create()
                .withHnswEfConstruction(invalidHnswEfConstruction)
                .toSettingsAccessor();

        assertInvalidEfConstruction(invalidHnswEfConstruction, settings);
    }

    private static void assertInvalidEfConstruction(int invalidHnswEfConstruction, SettingsAccessor settings) {
        IndexSettingRecordsByState validationRecords = validateAsInvalid(VALIDATOR, settings);
        assertInvalidValue(validationRecords, HNSW_EF_CONSTRUCTION, invalidHnswEfConstruction);
        assertThatThrownBy(() -> VALIDATOR.validateToTypedConfig(settings))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        HNSW_EF_CONSTRUCTION.getSettingName(),
                        "must be between 1 and",
                        String.valueOf(VERSION.maxHnswEfConstruction()));
    }
}
