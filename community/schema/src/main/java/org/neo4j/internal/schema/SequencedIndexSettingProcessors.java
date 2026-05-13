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

import java.util.Arrays;
import java.util.Collections;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.graphdb.schema.IndexSetting;

/// A sequenced collection of [IndexSettingsProcessor] that is itself an [IndexSettingsProcessor]
public class SequencedIndexSettingProcessors implements IndexSettingsProcessor {
    private final SequencedCollection<IndexSettingsProcessor> processors;
    private final SortedSet<IndexSetting> settings;

    /// Merges provided [IndexSettingsProcessor] into a single [IndexSettingsProcessor]
    public static SequencedIndexSettingProcessors mergeProcessors(IndexSettingsProcessor... processors) {
        SequencedCollection<IndexSettingsProcessor> sequencedProcessors = Arrays.asList(processors);
        SortedSet<IndexSetting> settings = new TreeSet<>(INDEX_SETTING_COMPARATOR);
        visitSettings(sequencedProcessors, settings::addAll, Consumers.ignoreValue());
        return new SequencedIndexSettingProcessors(sequencedProcessors, settings);
    }

    /// Merges provided [IndexSettingsProcessor] into a single [ValidatingIndexSettingsProcessor] providing
    /// there is at least one [ValidatingIndexSettingsProcessor] for each [IndexSetting]
    /// @see #mergeProcessors(IndexSettingsProcessor...)
    public static ValidatingIndexSettingsProcessor mergeToValidatingProcessor(IndexSettingsProcessor... processors) {
        SequencedCollection<IndexSettingsProcessor> sequencedProcessors = Arrays.asList(processors);
        SortedSet<IndexSetting> settings = new TreeSet<>(INDEX_SETTING_COMPARATOR);
        SortedSet<IndexSetting> validatedSettings = new TreeSet<>(INDEX_SETTING_COMPARATOR);
        visitSettings(sequencedProcessors, settings::addAll, validatedSettings::addAll);

        // by construction validatedSettings is a subset of settings
        if (validatedSettings.size() != settings.size()) {
            settings.removeAll(validatedSettings);
            SortedSet<String> settingNames = new TreeSet<>(CASE_INSENSITIVE_ORDER);
            for (IndexSetting setting : settings) {
                settingNames.add(setting.getSettingName());
            }

            throw new IllegalArgumentException(
                    "Provided %ss could not become a %s because the following settings do not have an associated %s: %s"
                            .formatted(
                                    IndexSettingsProcessor.class.getSimpleName(),
                                    ValidatingIndexSettingsProcessor.class.getSimpleName(),
                                    ValidatingIndexSettingsProcessor.class.getSimpleName(),
                                    settingNames));
        }

        IndexSettingsProcessor delegate = new SequencedIndexSettingProcessors(sequencedProcessors, settings);
        return new ValidatingIndexSettingsProcessor() {
            @Override
            public void updateForVerification(KnownIndexSettingRecords records) {
                delegate.updateForVerification(records);
            }

            @Override
            public void updateForAuthoritativeRead(KnownIndexSettingRecords records) {
                delegate.updateForAuthoritativeRead(records);
            }

            @Override
            public Set<IndexSetting> settings() {
                return delegate.settings();
            }
        };
    }

    private SequencedIndexSettingProcessors(
            SequencedCollection<IndexSettingsProcessor> processors, SortedSet<IndexSetting> settings) {
        this.processors = Collections.unmodifiableSequencedCollection(processors);
        this.settings = Collections.unmodifiableSortedSet(settings);
    }

    @Override
    public void updateForVerification(KnownIndexSettingRecords records) {
        for (IndexSettingsProcessor processor : processors) {
            processor.updateForVerification(records);
        }
    }

    @Override
    public void updateForAuthoritativeRead(KnownIndexSettingRecords records) {
        for (IndexSettingsProcessor processor : processors) {
            processor.updateForAuthoritativeRead(records);
        }
    }

    @Override
    public Set<IndexSetting> settings() {
        return settings;
    }

    private static void visitSettings(
            Iterable<IndexSettingsProcessor> processors,
            Consumer<Set<IndexSetting>> visitSettings,
            Consumer<Set<IndexSetting>> visitValidatedSettings) {
        for (IndexSettingsProcessor processor : processors) {
            visitSettings.accept(processor.settings());
            if (processor instanceof ValidatingIndexSettingsProcessor) {
                visitValidatedSettings.accept(processor.settings());
            }
        }
    }
}
