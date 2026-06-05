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
package org.neo4j.gqlstatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NonGqlStandardDiagnosticRecordPropertyTest {

    @Nested
    class Builder {
        @ParameterizedTest
        @ValueSource(
                strings = {
                    "_key",
                    "_other_key",
                    "__double_underscore_key",
                    "_CAPS-KEY",
                    "_snakeCaseKey",
                    "_strangeFoRmaTedKey?",
                    "_key with spaces, right?"
                })
        void shouldBuildPropertyStartingWithUnderscore(String key) {
            var property =
                    NonGqlStandardDiagnosticRecordProperty.Builder.fromKey(key).build();
            var value = "Value";

            assertThat(property.key()).isEqualTo(key);
            assertThat(property.disabled()).isFalse();
            assertThat(property.serializeValue(value)).isSameAs(value);
            assertThat(property.isValueOmitted(value)).isFalse();
            assertThat(property.defaultEntry()).isNotPresent();
            assertThat(property.defaultValue()).isNotPresent();
        }

        @ParameterizedTest
        @ValueSource(strings = {"key", " __double_underscore_key"})
        void shouldFailToBuildPropertyKeyNotStartingWithUnderscore(String key) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> NonGqlStandardDiagnosticRecordProperty.Builder.fromKey(key));
        }

        @Test
        void shouldBuildDisabledKeyProperty() {
            var property = NonGqlStandardDiagnosticRecordProperty.Builder.fromKey("_disabled_key")
                    .disabled()
                    .build();

            assertThat(property.disabled()).isTrue();
        }

        @Test
        void shouldBuildWithOmittedValueProperty() {
            var property = NonGqlStandardDiagnosticRecordProperty.Builder.fromKey("_custom_key")
                    .withValueOmittedPredicate(value -> value.equals("omitted"))
                    .build();
            var value = "Value";
            var omittedValue = "omitted";

            assertThat(property.isValueOmitted(value)).isFalse();
            assertThat(property.isValueOmitted(omittedValue)).isTrue();
        }

        @Test
        void shouldBuildWithSerializedValueProperty() {
            var property = NonGqlStandardDiagnosticRecordProperty.Builder.fromKey("_custom_key")
                    .withValueSerializer(value -> "Value: ".concat(value.toString()))
                    .build();

            var value = "the value";

            assertThat(property.serializeValue(value)).isEqualTo("Value: the value");
        }
    }
}
