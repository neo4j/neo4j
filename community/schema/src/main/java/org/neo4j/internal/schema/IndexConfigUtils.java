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
package org.neo4j.internal.schema;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.utils.PrettyPrinter.stringify;
import static org.neo4j.values.utils.ValueTypeNames.nameOfType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.exceptions.InternalException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.InclusiveRange;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.Invalid;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.Unprocessed;
import org.neo4j.internal.schema.IndexSettingRecord.UnrecognizedSetting;

public class IndexConfigUtils {
    public static final Comparator<IndexSetting> INDEX_SETTING_COMPARATOR =
            Comparator.comparing(IndexSetting::getSettingName, CASE_INSENSITIVE_ORDER);

    public static void assertValidRecords(
            IndexSettingRecordsByState validationRecords,
            IndexProviderDescriptor descriptor,
            Set<IndexSetting> acceptedSettings) {
        // fail on first
        Invalid invalidRecord = validationRecords.getFirstInvalidRecordOrNull();
        if (invalidRecord == null) {
            return;
        }

        String settingName = invalidRecord.settingName();
        throw switch (invalidRecord) {
            // these are an implementation mistake
            case Unprocessed unprocessed -> incompleteValidation(descriptor, unprocessed);
            case Pending pending -> incompleteValidation(descriptor, pending);

            // these are likely user mistakes
            case UnrecognizedSetting ignored -> unrecognizedSetting(settingName, acceptedSettings);

            case MissingSetting ignored -> InvalidArgumentException.missingInput("setting", settingName);

            case IncorrectType incorrectType -> {
                Object value = incorrectType.value();
                yield InvalidArgumentException.invalidType(
                        settingName, stringify(value), nameOfType(incorrectType.targetType()), nameOfType(value));
            }

            case InvalidValue invalidValue -> {
                Object value = Objects.requireNonNullElse(invalidValue.value(), NO_VALUE);
                String valueString = stringify(value);

                IndexSettingsRequirement<?> requirement = invalidValue.requirement();
                yield switch (requirement.get()) {
                    case InclusiveRange<?> range ->
                        InvalidArgumentException.outOfRange(
                                settingName,
                                valueString,
                                nameOfType(value),
                                stringify(range.min()),
                                stringify(range.max()));

                    default ->
                        InvalidArgumentException.invalidIndexInput(
                                valueString,
                                settingName,
                                "'%s' is an unsupported '%s'. Supported: %s"
                                        .formatted(valueString, settingName, requirement.supported()));
                };
            }
        };
    }

    @FunctionalInterface
    public interface NamedSetting {
        String settingName();
    }

    @FunctionalInterface
    public interface HasSetting extends NamedSetting {
        IndexSetting setting();

        @Override
        default String settingName() {
            return setting().getSettingName();
        }
    }

    public interface IndexSettingsRequirement<T> extends Supplier<T> {
        String supported();
    }

    private static InternalException incompleteValidation(
            IndexProviderDescriptor descriptor, IndexSettingRecord record) {
        return InternalException.indexNotApplicable(
                descriptor.name(),
                "Validation for '%s' is incomplete. Provided: %s".formatted(record.settingName(), record));
    }

    public static InvalidArgumentException unrecognizedSetting(String settingName, Set<IndexSetting> settings) {
        List<String> settingNames = new ArrayList<>(settings.size());
        for (IndexSetting setting : settings) {
            settingNames.addLast(setting.getSettingName());
        }
        return InvalidArgumentException.invalidIndexConfig(settingName, settingNames);
    }

    public static IllegalArgumentException duplicateSettings(
            String providedType, String discriminatingType, Set<String> duplicateSettingNames) {
        return new IllegalArgumentException(
                "Expected a single %s to be provided for each %s. Provided duplicates for: %s"
                        .formatted(
                                providedType,
                                discriminatingType,
                                Iterables.toString(duplicateSettingNames, ", ", "[", "]")));
    }
}
