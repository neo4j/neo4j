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
package org.neo4j.values.storable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class UnsupportedValueTest {

    @Test
    void shouldExposeDriverMetadata() {
        var value = Values.unsupportedValue("FancyType", "9.3", "from the future");

        assertThat(value.name()).isEqualTo("FancyType");
        assertThat(value.minProtocolVersion()).isEqualTo("9.3");
        assertThat(value.message()).contains("from the future");
        assertThat(value.getTypeName()).isEqualTo(UnsupportedValue.TYPE_NAME);
        assertThat(value.valueRepresentation()).isEqualTo(ValueRepresentation.UNKNOWN);
        assertThat(value.isIncomparableType()).isTrue();
    }

    @Test
    void shouldDefineEqualityAndHashOverAllFields() {
        var a = Values.unsupportedValue("FancyType", "9.3", "msg");
        var sameAsA = Values.unsupportedValue("FancyType", "9.3", "msg");
        var differentMessage = Values.unsupportedValue("FancyType", "9.3", null);
        var differentName = Values.unsupportedValue("OtherType", "9.3", "msg");

        assertThat(a).isEqualTo(sameAsA);
        assertThat(a.hashCode()).isEqualTo(sameAsA.hashCode());
        assertThat(a).isNotEqualTo(differentMessage);
        assertThat(a).isNotEqualTo(differentName);
    }

    @Test
    void shouldNotEqualOtherValueTypes() {
        var value = Values.unsupportedValue("FancyType", "9.3", null);

        assertThat(value.equals(Values.stringValue("FancyType"))).isFalse();
        assertThat(value.equals(Values.NO_VALUE)).isFalse();
    }

    @Test
    void shouldWriteItselfAsUnsupported() {
        var value = Values.unsupportedValue("FancyType", "9.3", "from the future");
        var writer = new CapturingWriter();

        value.writeTo(writer);

        assertThat(writer.typeName).isEqualTo("FancyType");
        assertThat(writer.minProtocolVersion).isEqualTo("9.3");
        assertThat(writer.message).isEqualTo("from the future");
    }

    @Test
    void shouldWriteNullMessageWhenAbsent() {
        var value = Values.unsupportedValue("FancyType", "9.3", null);
        var writer = new CapturingWriter();

        value.writeTo(writer);

        assertThat(writer.message).isNull();
    }

    @Test
    void shouldNotBeMappable() {
        var value = Values.unsupportedValue("FancyType", "9.3", null);

        // The mapper is never consulted; mapping an unsupported type fails outright.
        assertThatThrownBy(() -> value.map(null)).isInstanceOf(UnsupportedOperationException.class);
    }

    private static final class CapturingWriter extends ValueWriter.Adapter<RuntimeException> {
        private String typeName;
        private String minProtocolVersion;
        private String message;

        @Override
        public void writeUnsupported(String typeName, String minProtocolVersion, String message) {
            this.typeName = typeName;
            this.minProtocolVersion = minProtocolVersion;
            this.message = message;
        }
    }
}
