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
package org.neo4j.cypher.operations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.cypher.operations.CypherRuntimeParser.parseFloat32Vector;
import static org.neo4j.cypher.operations.CypherRuntimeParser.parseFloat64Vector;
import static org.neo4j.cypher.operations.CypherRuntimeParser.parseInt16Vector;
import static org.neo4j.cypher.operations.CypherRuntimeParser.parseInt32Vector;
import static org.neo4j.cypher.operations.CypherRuntimeParser.parseInt64Vector;
import static org.neo4j.cypher.operations.CypherRuntimeParser.parseInt8Vector;
import static org.neo4j.values.storable.Values.float32Vector;
import static org.neo4j.values.storable.Values.float64Vector;
import static org.neo4j.values.storable.Values.int16Vector;
import static org.neo4j.values.storable.Values.int32Vector;
import static org.neo4j.values.storable.Values.int64Vector;
import static org.neo4j.values.storable.Values.int8Vector;

import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InvalidArgumentException;

class CypherRuntimeParserTest {

    @Test
    void shouldParseInt8Vector() {
        assertThat(parseInt8Vector("[1, 2, 3, 4]")).isEqualTo(int8Vector((byte) 1, (byte) 2, (byte) 3, (byte) 4));
        assertThat(parseInt8Vector("[-128, 127]")).isEqualTo(int8Vector(Byte.MIN_VALUE, Byte.MAX_VALUE));
        assertThatThrownBy(() -> parseInt8Vector("[-129]")).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> parseInt8Vector("[128]")).isInstanceOf(ArithmeticException.class);
        assertThat(parseInt8Vector("[1, 2.0, 3.0, 4.0]")).isEqualTo(int8Vector((byte) 1, (byte) 2, (byte) 3, (byte) 4));
        assertThatThrownBy(() -> parseInt8Vector("[1, 2, NULL, 4]")).isInstanceOf(CypherTypeException.class);
        assertThatThrownBy(() -> parseInt8Vector("NULL")).isInstanceOf(CypherTypeException.class);
    }

    @Test
    void shouldParseInt16Vector() {
        assertThat(parseInt16Vector("[1, 2, 3, 4]")).isEqualTo(int16Vector((short) 1, (short) 2, (short) 3, (short) 4));
        assertThat(parseInt16Vector("[-32768, 32767]")).isEqualTo(int16Vector(Short.MIN_VALUE, Short.MAX_VALUE));
        assertThatThrownBy(() -> parseInt16Vector("[-32769]")).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> parseInt16Vector("[32768]")).isInstanceOf(ArithmeticException.class);
        assertThat(parseInt16Vector("[1, 2.0, 3.0, 4.0]"))
                .isEqualTo(int16Vector((short) 1, (short) 2, (short) 3, (short) 4));
        assertThatThrownBy(() -> parseInt16Vector("[1, 2, NULL, 4]")).isInstanceOf(CypherTypeException.class);
        assertThatThrownBy(() -> parseInt16Vector("NULL")).isInstanceOf(CypherTypeException.class);
    }

    @Test
    void shouldParseInt32Vector() {
        assertThat(parseInt32Vector("[1, 2, 3, 4]")).isEqualTo(int32Vector(1, 2, 3, 4));
        assertThat(parseInt32Vector("[-2147483648,  2147483647]"))
                .isEqualTo(int32Vector(Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertThatThrownBy(() -> parseInt32Vector("[-2147483649]")).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> parseInt32Vector("[2147483648]")).isInstanceOf(ArithmeticException.class);
        assertThat(parseInt32Vector("[1, 2.0, 3.0, 4.0]")).isEqualTo(int32Vector(1, 2, 3, 4));
        assertThatThrownBy(() -> parseInt32Vector("[1, 2, NULL, 4]")).isInstanceOf(CypherTypeException.class);
        assertThatThrownBy(() -> parseInt32Vector("NULL")).isInstanceOf(CypherTypeException.class);
    }

    @Test
    void shouldParseInt64Vector() {
        assertThat(parseInt64Vector("[1, 2, 3, 4]")).isEqualTo(int64Vector(1, 2, 3, 4));
        assertThat(parseInt64Vector("[-9223372036854775808, -2147483648, 2147483647, 9223372036854775807]"))
                .isEqualTo(int64Vector(Long.MIN_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE));
        assertThat(parseInt64Vector("[1, 2.0, 3.0, 4.0]")).isEqualTo(int64Vector(1, 2, 3, 4));
        assertThatThrownBy(() -> parseInt64Vector("[1, 2, NULL, 4]")).isInstanceOf(CypherTypeException.class);
        assertThatThrownBy(() -> parseInt64Vector("NULL")).isInstanceOf(CypherTypeException.class);
    }

    @Test
    void shouldParseFloat32Vector() {
        assertThat(parseFloat32Vector("[1.0, 2.0, 3.0, 4.0]")).isEqualTo(float32Vector(1.0f, 2.0f, 3.0f, 4.0f));
        assertThat(parseFloat32Vector("[1.4e-45, 3.4028235e+38]"))
                .isEqualTo(float32Vector(Float.MIN_VALUE, Float.MAX_VALUE));
        assertThat(parseFloat32Vector("[1, 2.0, 3.0, 4.0]")).isEqualTo(float32Vector(1, 2, 3, 4));
        assertThatThrownBy(() -> parseFloat32Vector("[1.0, 2.0, NULL, 4.0]")).isInstanceOf(CypherTypeException.class);
        assertThatThrownBy(() -> parseFloat32Vector("NULL")).isInstanceOf(CypherTypeException.class);

        assertThatThrownBy(() -> parseFloat32Vector("[1.7976931348623157E308]"))
                .isInstanceOf(InvalidArgumentException.class);
        assertThatThrownBy(() -> parseFloat32Vector("[-1.7976931348623157E308]"))
                .isInstanceOf(InvalidArgumentException.class);
    }

    @Test
    void shouldParseFloat64Vector() {
        assertThat(parseFloat64Vector("[1.0, 2.0, 3.0, 4.0]")).isEqualTo(float64Vector(1.0, 2.0, 3.0, 4.0));
        assertThat(parseFloat64Vector("[4.9E-324, 1.7976931348623157E308]"))
                .isEqualTo(float64Vector(Double.MIN_VALUE, Double.MAX_VALUE));
        assertThat(parseFloat64Vector("[1, 2.0, 3.0, 4.0]")).isEqualTo(float64Vector(1, 2, 3, 4));
        assertThatThrownBy(() -> parseFloat64Vector("[1.0, 2.0, NULL, 4.0]")).isInstanceOf(CypherTypeException.class);
        assertThatThrownBy(() -> parseFloat64Vector("NULL")).isInstanceOf(CypherTypeException.class);

        assertThatThrownBy(() -> parseFloat32Vector("[10.7976931348623157E308]"))
                .isInstanceOf(InvalidArgumentException.class);
        assertThatThrownBy(() -> parseFloat32Vector("[-10.7976931348623157E308]"))
                .isInstanceOf(InvalidArgumentException.class);
    }
}
