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
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.INDEX_SETTING_COMPARATOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.assertValidRecords;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.toIndexConfig;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.toValidSettings;

import java.util.Comparator;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.set.sorted.ImmutableSortedSet;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigValidationRecords;
import org.neo4j.internal.schema.IndexConfigValidationWrapper;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.IndexSettingValidators.IndexSettingValidator;
import org.neo4j.kernel.api.impl.schema.vector.IndexSettingValidators.ReadDefaultOnly;
import org.neo4j.values.storable.Value;

public interface VectorIndexSettingsValidator {

    IndexConfigValidationRecords validate(SettingsAccessor settings);

    default VectorIndexConfig validateToVectorIndexConfig(SettingsAccessor settings) {
        return validateToVectorIndexConfig(settings, validate(settings));
    }

    VectorIndexConfig validateToVectorIndexConfig(
            SettingsAccessor settings, IndexConfigValidationRecords validationRecords);

    VectorIndexConfig trustIsValidToVectorIndexConfig(SettingsAccessor settings);

    VectorIndexConfig trustIsValidToVectorIndexConfig(IndexConfigValidationRecords validationRecords);

    ImmutableSortedSet<IndexSetting> validSettings();

    class Validators implements VectorIndexSettingsValidator {
        private final VectorIndexVersion version;
        private final ImmutableSortedSet<IndexSettingValidator<? extends Value, ?>> validators;
        private final ImmutableSortedSet<IndexSetting> validSettings;
        private final ImmutableSortedSet<String> validSettingNames;
        private final ImmutableSortedSet<String> handledSettingNames;

        @SafeVarargs
        Validators(VectorIndexVersion version, IndexSettingValidator<? extends Value, ?>... validators) {
            this.version = version;

            // check we've not passed multiple validators for the same setting
            final var seenSettingNames = Sets.mutable.<String>withInitialCapacity(validators.length);
            final var checkedValidators =
                    Lists.mutable.<IndexSettingValidator<? extends Value, ?>>withInitialCapacity(validators.length);
            for (final var validator : validators) {
                if (!seenSettingNames.add(validator.setting().getSettingName())) {
                    throw new IllegalStateException("Expected a single %s to be provided for '%s', multiple given."
                            .formatted(
                                    IndexSettingValidator.class.getSimpleName(),
                                    validator.setting().getSettingName()));
                }
                checkedValidators.add(validator);
            }

            this.validators = checkedValidators.toImmutableSortedSet(
                    Comparator.comparing(validator -> validator.setting().getSettingName(), CASE_INSENSITIVE_ORDER));

            final var handledSettings =
                    this.validators.collect(IndexSettingValidator::setting).toSet();
            this.handledSettingNames =
                    handledSettings.collect(IndexSetting::getSettingName).toImmutableSortedSet(CASE_INSENSITIVE_ORDER);

            this.validSettings = handledSettings
                    .difference(this.validators
                            .asLazy()
                            .selectInstancesOf(ReadDefaultOnly.class)
                            .collect(IndexSettingValidator::setting)
                            .toSet())
                    .toImmutableSortedSet(INDEX_SETTING_COMPARATOR);

            this.validSettingNames = this.validSettings
                    .collect(IndexSetting::getSettingName)
                    .toImmutableSortedSet(CASE_INSENSITIVE_ORDER);
        }

        @Override
        public IndexConfigValidationRecords validate(SettingsAccessor settings) {
            final var validationRecords =
                    IndexConfigValidationWrapper.validateSettingNames(settings.settingNames(), handledSettingNames);
            validators
                    .asLazy()
                    .collect(validator -> validator.validate(settings))
                    .forEach(validationRecords::with);
            return validationRecords;
        }

        @Override
        public VectorIndexConfig validateToVectorIndexConfig(
                SettingsAccessor settings, IndexConfigValidationRecords validationRecords) {
            assertValidRecords(validationRecords, version.descriptor(), validSettingNames);
            final var validRecords = validationRecords.validRecords();
            return new VectorIndexConfig(
                    version,
                    toIndexConfig(validRecords, validSettingNames),
                    toValidSettings(validRecords),
                    validSettingNames,
                    handledSettingNames);
        }

        @Override
        public VectorIndexConfig trustIsValidToVectorIndexConfig(SettingsAccessor settings) {
            final var validRecords = validators.collect(validator -> validator.trustIsValid(settings));
            return new VectorIndexConfig(
                    version,
                    toIndexConfig(validRecords),
                    toValidSettings(validRecords),
                    validSettingNames,
                    handledSettingNames);
        }

        @Override
        public VectorIndexConfig trustIsValidToVectorIndexConfig(IndexConfigValidationRecords validationRecords) {
            final var validRecords = validationRecords.validRecords();
            return new VectorIndexConfig(
                    version,
                    toIndexConfig(validRecords),
                    toValidSettings(validRecords),
                    validSettingNames,
                    handledSettingNames);
        }

        @Override
        public ImmutableSortedSet<IndexSetting> validSettings() {
            return validSettings;
        }
    }

    class ValidatorNotFound implements VectorIndexSettingsValidator {
        private final IllegalStateException exception;

        ValidatorNotFound(IllegalStateException exception) {
            this.exception = exception;
        }

        @Override
        public IndexConfigValidationRecords validate(SettingsAccessor settings) {
            throw exception;
        }

        @Override
        public VectorIndexConfig validateToVectorIndexConfig(SettingsAccessor settings) {
            throw exception;
        }

        @Override
        public VectorIndexConfig validateToVectorIndexConfig(
                SettingsAccessor settings, IndexConfigValidationRecords validationRecords) {
            throw exception;
        }

        @Override
        public VectorIndexConfig trustIsValidToVectorIndexConfig(SettingsAccessor settings) {
            throw exception;
        }

        @Override
        public VectorIndexConfig trustIsValidToVectorIndexConfig(IndexConfigValidationRecords validationRecords) {
            throw exception;
        }

        @Override
        public ImmutableSortedSet<IndexSetting> validSettings() {
            return SortedSets.immutable.empty();
        }
    }

    class ValidatorNotFoundForKernelVersion extends ValidatorNotFound {
        ValidatorNotFoundForKernelVersion(VectorIndexVersion version, KernelVersion kernelVersion) {
            super(new IllegalStateException("%s not found for '%s' on '%s'."
                    .formatted(
                            VectorIndexSettingsValidator.class.getSimpleName(),
                            version.descriptor().name(),
                            kernelVersion)));
        }
    }
}
