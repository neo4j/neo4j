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
import static org.neo4j.internal.schema.IndexConfigUtils.INDEX_SETTING_COMPARATOR;
import static org.neo4j.internal.schema.IndexConfigUtils.duplicateSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.UnrecognizedSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsProcessor.ValidatingIndexSettingsProcessor;

public class DefaultIndexSettingsValidator implements IndexSettingsValidator {
    private final IndexSettingExtractors extractors;
    private final ValidatingIndexSettingsProcessor processor;
    private final List<IndexSettingEntry> implicitSettings;

    /// @param extractors the collection of [IndexSettingExtractor]s used to extract the initial settings from a
    /// [SettingsAccessor]
    /// @param processor the processor that validates transforms those extracted settings into their typed final
    /// values. Must cover all [IndexSetting]s extracted by the `extractors`.
    /// @param implicitSettings any settings that are not expected, and thus an error, in the original authoritative
    // set of valid settings, that are now expected with their implicit value.
    /// @see SequencedIndexSettingProcessors#mergeToValidatingProcessor(IndexSettingsProcessor...)
    public DefaultIndexSettingsValidator(
            IndexSettingExtractors extractors,
            ValidatingIndexSettingsProcessor processor,
            IndexSettingEntry... implicitSettings) {
        assertProcessorCoversAllExtractors(extractors, processor);
        assertNoDuplicateImplicitSettings(implicitSettings);
        assertNoDuplicateHandledSettings(processor, implicitSettings);

        this.extractors = extractors;
        this.processor = processor;
        this.implicitSettings = Collections.unmodifiableList(Arrays.asList(implicitSettings)); // avoid copy
    }

    private static void assertProcessorCoversAllExtractors(
            IndexSettingExtractors extractors, ValidatingIndexSettingsProcessor processor) {
        Set<IndexSetting> settings = processor.settings();
        Set<String> missingSettings = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        for (IndexSetting setting : extractors.settings()) {
            if (!settings.contains(setting)) {
                missingSettings.add(setting.getSettingName());
            }
        }
        if (!missingSettings.isEmpty()) {
            throw new IllegalArgumentException(
                    "Provided %s does not cover all of the %s of the provided %s. Missing: %s"
                            .formatted(
                                    ValidatingIndexSettingsProcessor.class.getSimpleName(),
                                    IndexSetting.class.getSimpleName(),
                                    IndexSettingExtractors.class.getSimpleName(),
                                    Iterables.toString(missingSettings, ", ", "[", "]")));
        }
    }

    private static void assertNoDuplicateImplicitSettings(IndexSettingEntry... implicitSettings) {
        Set<String> duplicateSettings = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        Set<IndexSetting> seenImplicitSettings = new TreeSet<>(INDEX_SETTING_COMPARATOR);
        for (IndexSettingEntry implicit : implicitSettings) {
            IndexSetting setting = implicit.setting();
            if (!seenImplicitSettings.add(setting)) {
                duplicateSettings.add(setting.getSettingName());
            }
        }
        if (!duplicateSettings.isEmpty()) {
            throw duplicateSettings(IndexSetting.class.getSimpleName(), "setting injected", duplicateSettings);
        }
    }

    private static void assertNoDuplicateHandledSettings(
            ValidatingIndexSettingsProcessor processor, IndexSettingEntry... implicitSettings) {
        Set<IndexSetting> settings = processor.settings();
        Set<String> handledSettings = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        for (IndexSettingEntry implicit : implicitSettings) {
            IndexSetting setting = implicit.setting();
            if (settings.contains(setting)) {
                handledSettings.add(setting.getSettingName());
            }
        }
        if (!handledSettings.isEmpty()) {
            throw new IllegalArgumentException(
                    "Provided implicit settings for injection contains %s handled by the %s. Settings: %s"
                            .formatted(
                                    IndexSetting.class.getSimpleName(),
                                    ValidatingIndexSettingsProcessor.class.getSimpleName(),
                                    Iterables.toString(handledSettings, ", ", "[", "]")));
        }
    }

    public IndexSettingRecordsByState validate(SettingsAccessor accessor) {
        Set<String> expectedSettingNames = extractors.settingNames();
        Collection<UnrecognizedSetting> unrecognizedSettings = new ArrayList<>();
        for (String settingName : accessor.settingNames()) {
            if (!expectedSettingNames.contains(settingName)) {
                unrecognizedSettings.add(new UnrecognizedSetting(settingName));
            }
        }

        KnownIndexSettingRecords recordWithSettings = extractors.extractForValidation(accessor);
        processor.updateForVerification(recordWithSettings);

        for (IndexSettingEntry entry : implicitSettings) {
            recordWithSettings.upsert(new Valid(entry));
        }

        IndexSettingRecords records = recordWithSettings.toIndexSettingRecords();
        records.upsertAll(unrecognizedSettings); // unrecognized settings should overwrite and take precedence
        return records.groupByState();
    }

    @Override
    public Iterable<Valid> interpretAuthoritative(SettingsAccessor accessor) {
        KnownIndexSettingRecords records = extractors.extractForAuthoritativeRead(accessor);
        processor.updateForAuthoritativeRead(records);

        SortedSet<Valid> validRecords = new TreeSet<>();
        for (IndexSettingRecord record : records) {
            switch (record) {
                case Valid valid -> validRecords.add(valid);
                case null, default ->
                    throw new IllegalStateException("%s was trusted to be %s but was %s."
                            .formatted(
                                    record,
                                    Valid.class.getSimpleName(),
                                    record != null ? record.getClass().getSimpleName() : null));
            }
        }

        for (IndexSettingEntry entry : implicitSettings) {
            validRecords.add(new Valid(entry));
        }

        return validRecords;
    }

    @Override
    public Set<IndexSetting> acceptedSettings() {
        return extractors.settings();
    }
}
