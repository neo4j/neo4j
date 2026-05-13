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
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigValidationTestUtils.assertInvalidArgumentExceptionThrownBy;

import java.util.Arrays;
import java.util.Optional;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexSettingRecordsByState;
import org.neo4j.internal.schema.NotFoundTypedIndexSettingsValidator;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.TypedIndexSettingsValidator;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.test.arguments.KernelVersionSource;

public class VectorIndexUnknownConfigValidationTest {
    private static final Iterable<ThrowingConsumer<TypedIndexSettingsValidator<VectorIndexConfig>>> OPERATIONS =
            Arrays.asList(
                    v -> v.validate(mock()),
                    v -> v.validateToTypedConfig(mock(SettingsAccessor.class)),
                    v -> v.validateToTypedConfig(mock(IndexSettingRecordsByState.class)),
                    v -> v.interpretAuthoritative(mock(SettingsAccessor.class)),
                    v -> v.interpretAuthoritativeToTypedConfig(mock(SettingsAccessor.class)),
                    v -> v.interpretAuthoritativeToTypedConfig(Iterables.empty()));

    @Test
    void unknownLatestVectorIndexVersionValidation() {
        VectorIndexVersion unknown = VectorIndexVersion.UNKNOWN;
        assertValidatorForVersion(unknown.descriptor(), Optional.empty(), unknown.indexSettingValidator());
    }

    @ParameterizedTest
    @KernelVersionSource
    void unknownVectorIndexVersionValidation(KernelVersion kernelVersion) {
        VectorIndexVersion unknown = VectorIndexVersion.UNKNOWN;
        assertValidatorForVersion(unknown.descriptor(), Optional.empty(), unknown.indexSettingValidator(kernelVersion));
    }

    @ParameterizedTest
    @KernelVersionSource(lessThan = "5.11")
    void unknownValidationForVectorIndexV1(KernelVersion kernelVersion) {
        assertValidatorForVersion(VectorIndexVersion.V1_0, kernelVersion);
    }

    @ParameterizedTest
    @KernelVersionSource(lessThan = "5.18")
    void unknownValidationForVectorIndexV2(KernelVersion kernelVersion) {
        assertValidatorForVersion(VectorIndexVersion.V2_0, kernelVersion);
    }

    @ParameterizedTest
    @KernelVersionSource(lessThan = "2025.09")
    void unknownValidationForVectorIndexV3(KernelVersion kernelVersion) {
        assertValidatorForVersion(VectorIndexVersion.V3_0, kernelVersion);
    }

    private static void assertValidatorForVersion(VectorIndexVersion version, KernelVersion kernelVersion) {
        assertValidatorForVersion(
                version.descriptor(), Optional.of(kernelVersion), version.indexSettingValidator(kernelVersion));
    }

    private static void assertValidatorForVersion(
            IndexProviderDescriptor descriptor,
            Optional<KernelVersion> kernelVersion,
            TypedIndexSettingsValidator<VectorIndexConfig> validator) {
        assertThat(validator).isInstanceOf(NotFoundTypedIndexSettingsValidator.class);
        assertThat(validator.acceptedSettings()).isEmpty();
        assertThat(OPERATIONS).allSatisfy(op -> {
            var validatorNotFoundAssert = assertInvalidArgumentExceptionThrownBy(() -> op.accept(validator))
                    .hasMessageContainingAll("Validator not found for", descriptor.name());
            kernelVersion.ifPresent(kv -> validatorNotFoundAssert.hasMessageContainingAll("on", kv.toString()));
        });
    }
}
