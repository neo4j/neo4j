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
package org.neo4j.logging.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.logging.internal.LogMessageUtil.slf4jToStringFormatPlaceholders;

import org.junit.jupiter.api.Test;

class LogMessageUtilTest {
    @Test
    void shouldThrowWhenStringIsNull() {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> slf4jToStringFormatPlaceholders(null));
    }

    @Test
    void shouldDoNothingForEmptyString() {
        assertThat(slf4jToStringFormatPlaceholders("")).isEmpty();
    }

    @Test
    void shouldDoNothingForStringWithoutPlaceholders() {
        assertThat(slf4jToStringFormatPlaceholders("Simple log message")).isEqualTo("Simple log message");
    }

    @Test
    void shouldReplaceSlf4jPlaceholderWithStringFormatPlaceholder() {
        assertThat(slf4jToStringFormatPlaceholders("Log message with {} single placeholder"))
                .isEqualTo("Log message with %s single placeholder");
        assertThat(slf4jToStringFormatPlaceholders("Log message {} with two {} placeholders"))
                .isEqualTo("Log message %s with two %s placeholders");
        assertThat(slf4jToStringFormatPlaceholders("Log {} message {} with three {} placeholders"))
                .isEqualTo("Log %s message %s with three %s placeholders");
    }
}
