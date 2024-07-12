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
package org.neo4j.genai.vector.providers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.genai.util.Parameters;
import org.neo4j.genai.util.ParametersTest;
import org.neo4j.genai.vector.VectorEncoding;
import org.neo4j.genai.vector.VectorEncoding.Provider;

@TestInstance(Lifecycle.PER_CLASS)
abstract class ConfigurationTestBase<PARAMETERS> {
    protected final Provider<PARAMETERS> provider;

    protected final Collection<RequiredSetting> requiredSettings;
    protected final Collection<OptionalSetting> optionalSettings;
    protected final Collection<Setting> settings;
    protected final Models models;

    protected final Map<String, Object> minimalConfig;

    protected final Map<String, Object> fullConfig;

    protected Map<String, Object> config;

    protected ConfigurationTestBase(
            Provider<PARAMETERS> provider,
            Collection<RequiredSetting> requiredSettings,
            Collection<OptionalSetting> optionalSettings,
            Models models) {
        this.provider = provider;
        this.requiredSettings = requiredSettings;
        this.optionalSettings = optionalSettings;
        this.settings = new ArrayList<>();
        this.settings.addAll(this.requiredSettings);
        this.settings.addAll(this.optionalSettings);
        this.minimalConfig =
                requiredSettings.stream().collect(Collectors.toUnmodifiableMap(Setting::name, Setting::valid));
        this.fullConfig = this.settings.stream().collect(Collectors.toUnmodifiableMap(Setting::name, Setting::valid));
        this.models = new Models(
                models.setting(),
                models.type(),
                Collections.unmodifiableCollection(models.supported()),
                Collections.unmodifiableCollection(models.unsupported()));
    }

    protected Provider.Encoder configure(Map<String, ?> config) {
        return provider.configure(Parameters.parse(provider.parameterDeclarations(), ParametersTest.from(config)));
    }

    protected boolean noRequiredSettings() {
        return requiredSettings.isEmpty();
    }

    protected boolean noOptionalSettings() {
        return optionalSettings.isEmpty();
    }

    protected boolean noSettings() {
        return settings.isEmpty();
    }

    protected boolean noSupportedModels() {
        return models.supported.isEmpty();
    }

    protected boolean noUnsupportedModels() {
        return models.unsupported.isEmpty();
    }

    private Collection<RequiredSetting> requiredSettings() {
        return requiredSettings;
    }

    private Collection<OptionalSetting> optionalSettings() {
        return optionalSettings;
    }

    private Collection<Setting> settings() {
        return settings;
    }

    @BeforeEach
    void setup() {
        config = new HashMap<>(minimalConfig);
    }

    @Test
    void defaultConfiguration() {
        assertThatCode(() -> configure(minimalConfig)).as("rely on defaults").doesNotThrowAnyException();
    }

    @DisabledIf(value = "noRequiredSettings", disabledReason = "there are no required settings")
    @ParameterizedTest
    @MethodSource("requiredSettings")
    void requiredSettingShouldBeIncludedInRequiredConfigTypeString(Setting setting) {
        assertThat(requiredConfigType(provider)).contains("%s :: %s".formatted(setting.name(), setting.cypherType()));
    }

    @DisabledIf(value = "noOptionalSettings", disabledReason = "there are no optional settings")
    @ParameterizedTest
    @MethodSource("optionalSettings")
    void optionalSettingShouldBeIncludedInOptionalConfigTypeString(Setting setting) {
        assertThat(optionalConfigType(provider)).contains("%s :: %s".formatted(setting.name(), setting.cypherType()));
    }

    @DisabledIf(value = "noOptionalSettings", disabledReason = "there are no optional settings")
    @ParameterizedTest
    @MethodSource("optionalSettings")
    void optionalSettingsWithDefaultsShouldBeIncludedInDefaultMap(OptionalSetting setting) {
        setting.defaultValue.ifPresentOrElse(
                defaultValue -> {
                    assertThat(defaultConfig(provider)).containsEntry(setting.name(), defaultValue);
                },
                () -> {
                    assertThat(defaultConfig(provider)).doesNotContainKey(setting.name());
                });
    }

    @DisabledIf(value = "noRequiredSettings", disabledReason = "there are no required settings")
    @ParameterizedTest
    @MethodSource
    void requiredSettings(Setting setting) {
        final var name = setting.name();
        config.remove(name);
        assertThatThrownBy(() -> configure(config), "missing %s", name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(name, "is expected to have been set");
    }

    @DisabledIf(value = "noSettings", disabledReason = "there are no settings")
    @ParameterizedTest
    @MethodSource
    void settings(Setting setting) {
        final var name = setting.name();
        config.put(name, setting.valid());
        assertThatCode(() -> configure(config)).as("correctly typed %s", name).doesNotThrowAnyException();

        config.put(name, setting.invalid());
        assertThatThrownBy(() -> configure(config), "incorrectly typed %s", name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(name, "is expected to have been of", setting.cypherType());
    }

    @DisabledIf(value = "noSupportedModels", disabledReason = "there are no supported models")
    @ParameterizedTest(name = "{1}")
    @MethodSource
    void supportedModels(String setting, Object model) {
        config.put(setting, model);
        assertThatCode(() -> configure(config)).as("supported model").doesNotThrowAnyException();
    }

    Stream<Arguments> supportedModels() {
        return models.supported().stream().map(model -> Arguments.of(models.setting, model));
    }

    @DisabledIf(value = "noUnsupportedModels", disabledReason = "there are no unsupported models")
    @ParameterizedTest(name = "{1}")
    @MethodSource
    void unsupportedModels(String setting, Object model) {
        config.put(setting, model);
        assertThatThrownBy(() -> configure(config), "unsupported model").isInstanceOf(IllegalArgumentException.class);
    }

    Stream<Arguments> unsupportedModels() {
        return models.unsupported().stream().map(model -> Arguments.of(models.setting, model));
    }

    protected interface Setting {
        String name();

        Class<?> type();

        String cypherType();

        Object valid();

        Object invalid();
    }

    protected record RequiredSetting(String name, Class<?> type, String cypherType, Object valid, Object invalid)
            implements Setting {
        @Override
        public String toString() {
            return name + ": " + type;
        }
    }

    protected record OptionalSetting(
            String name, Class<?> type, String cypherType, Object valid, Object invalid, Optional<?> defaultValue)
            implements Setting {
        @Override
        public String toString() {
            return name + ": " + type;
        }
    }

    protected record Models(String setting, Class<?> type, Collection<?> supported, Collection<?> unsupported) {
        public static Models IMPLICIT = new Models(null, null, List.of(), List.of());
    }

    private static String requiredConfigType(Provider<?> provider) {
        return VectorEncoding.ProviderRow.from(provider).requiredConfigType();
    }

    private static String optionalConfigType(Provider<?> provider) {
        return VectorEncoding.ProviderRow.from(provider).optionalConfigType();
    }

    private static Map<String, Object> defaultConfig(Provider<?> provider) {
        return VectorEncoding.ProviderRow.from(provider).defaultConfig();
    }
}
