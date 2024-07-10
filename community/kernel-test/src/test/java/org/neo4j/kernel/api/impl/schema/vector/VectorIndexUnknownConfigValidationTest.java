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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.neo4j.internal.schema.IndexConfigValidationRecords;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexSettingsValidator.ValidatorNotFound;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexSettingsValidator.ValidatorNotFoundForKernelVersion;
import org.neo4j.test.arguments.KernelVersionSource;

public class VectorIndexUnknownConfigValidationTest {

    private static <T> T any() {
        return null;
    }

    private static <T> T any(Class<T> cls) {
        return null;
    }

    @Test
    void unknownVectorIndexVersionValidation() {
        final var version = VectorIndexVersion.UNKNOWN;
        final var validator = version.indexSettingValidator();
        assertThat(validator).isInstanceOf(ValidatorNotFound.class);

        assertThatThrownBy(() -> validator.validate(any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name());

        assertThatThrownBy(() -> validator.validateToVectorIndexConfig(any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name());

        assertThatThrownBy(() -> validator.validateToVectorIndexConfig(any(), any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name());

        assertThatThrownBy(() -> validator.trustIsValidToVectorIndexConfig(any(SettingsAccessor.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name());

        assertThatThrownBy(() -> validator.trustIsValidToVectorIndexConfig(any(IndexConfigValidationRecords.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name());

        assertThat(validator.validSettings().castToSortedSet()).isEmpty();
    }

    @ParameterizedTest
    @KernelVersionSource(lessThan = "5.11")
    void unknownValidationForVectorIndexV1(KernelVersion kernelVersion) {
        final var version = VectorIndexVersion.V1_0;
        final var validator = version.indexSettingValidator(kernelVersion);
        assertThat(validator).isInstanceOf(ValidatorNotFoundForKernelVersion.class);

        assertThatThrownBy(() -> validator.validate(any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.validateToVectorIndexConfig(any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.validateToVectorIndexConfig(any(), any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.trustIsValidToVectorIndexConfig(any(SettingsAccessor.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.trustIsValidToVectorIndexConfig(any(IndexConfigValidationRecords.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());
    }

    @ParameterizedTest
    @KernelVersionSource(lessThan = "5.18")
    void unknownValidationForVectorIndexV2(KernelVersion kernelVersion) {
        final var version = VectorIndexVersion.V2_0;
        final var validator = version.indexSettingValidator(kernelVersion);
        assertThat(validator).isInstanceOf(ValidatorNotFoundForKernelVersion.class);

        assertThatThrownBy(() -> validator.validate(any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.validateToVectorIndexConfig(any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.validateToVectorIndexConfig(any(), any()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.trustIsValidToVectorIndexConfig(any(SettingsAccessor.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThatThrownBy(() -> validator.trustIsValidToVectorIndexConfig(any(IndexConfigValidationRecords.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        VectorIndexSettingsValidator.class.getSimpleName(),
                        "not found for",
                        version.descriptor().name(),
                        "on",
                        kernelVersion.toString());

        assertThat(validator.validSettings().castToSortedSet()).isEmpty();
    }
}
