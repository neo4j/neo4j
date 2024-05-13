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
package org.neo4j.cypher.internal.literal.interpreter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.cypher.internal.ast.factory.ASTFactory;

@SuppressWarnings("ConstantConditions")
public class LiteralInterpreterTest {
    private final ASTFactory.NULL POS = null;

    @Test
    void shouldInterpretNumbers() {
        final var x = new LiteralInterpreter();

        assertEquals(0L, x.newDecimalInteger(POS, "0", false));
        assertEquals(12345L, x.newDecimalInteger(POS, "12345", false));
        assertEquals(-12345L, x.newDecimalInteger(POS, "12345", true));
        assertEquals(Long.MAX_VALUE, x.newDecimalInteger(POS, Long.toString(Long.MAX_VALUE), false));
        assertEquals(
                Long.MIN_VALUE,
                x.newDecimalInteger(POS, Long.toString(Long.MIN_VALUE).substring(1), true));

        // old syntax
        assertEquals(8L, x.newOctalInteger(POS, "010", false));
        assertEquals(-8L, x.newOctalInteger(POS, "010", true));
        assertEquals(
                Long.MIN_VALUE,
                x.newOctalInteger(POS, "0" + Long.toString(Long.MIN_VALUE, 8).substring(1), true));

        assertEquals(8L, x.newOctalInteger(POS, "0o10", false));
        assertEquals(-8L, x.newOctalInteger(POS, "0o10", true));
        assertEquals(
                Long.MIN_VALUE,
                x.newOctalInteger(POS, "0o" + Long.toString(Long.MIN_VALUE, 8).substring(1), true));

        assertEquals(255L, x.newHexInteger(POS, "0xff", false));
        assertEquals(-255L, x.newHexInteger(POS, "0xff", true));
        assertEquals(
                Long.MIN_VALUE,
                x.newHexInteger(POS, "0x" + Long.toString(Long.MIN_VALUE, 16).substring(1), true));

        assertEquals(0.0d, x.newDouble(POS, "0.0"));
        assertEquals(0.0d, x.newDouble(POS, "0.0e0"));
        assertEquals(-0.0d, x.newDouble(POS, "-0.0e0"));
        assertEquals(1.0d, x.newDouble(POS, "1.0e0"));
        assertEquals(98723.0e31d, x.newDouble(POS, "98723.0e31"));
        assertEquals(Double.MAX_VALUE, x.newDouble(POS, Double.toString(Double.MAX_VALUE)));
        assertEquals(Double.MIN_VALUE, x.newDouble(POS, Double.toString(Double.MIN_VALUE)));
    }

    @Test
    void shouldInterpretString() {
        final var x = new LiteralInterpreter();

        assertEquals("a string", x.newString(POS, POS, "a string"));
        assertEquals("ÅÄü", x.newString(POS, POS, "ÅÄü"));
        assertEquals("Ελληνικά", x.newString(POS, POS, "Ελληνικά"));
        assertEquals("\uD83D\uDCA9", x.newString(POS, POS, "\uD83D\uDCA9"));
    }

    @Test
    void shouldInterpretNull() {
        final var x = new LiteralInterpreter();

        assertNull(x.newNullLiteral(POS));
    }

    @Test
    void shouldInterpretBoolean() {
        final var x = new LiteralInterpreter();

        assertEquals(true, x.newTrueLiteral(POS));
        assertEquals(false, x.newFalseLiteral(POS));
    }

    @Test
    void shouldInterpretInfinity() {
        final var x = new LiteralInterpreter();

        assertEquals(Double.POSITIVE_INFINITY, x.newInfinityLiteral(POS));
    }

    @Test
    void shouldInterpretNaN() {
        final var x = new LiteralInterpreter();

        assertEquals(Double.NaN, x.newNaNLiteral(POS));
    }

    @Test
    void shouldInterpretList() {
        final var x = new LiteralInterpreter();

        assertEquals(List.of(1, 2, 3), x.listLiteral(POS, List.of(1, 2, 3)));
    }

    @Test
    void shouldInterpretMap() {
        final var x = new LiteralInterpreter();

        assertEquals(Map.of(), x.mapLiteral(POS, List.of(), List.of()));
        assertEquals(Map.of("1", 1), x.mapLiteral(POS, List.of(stringPos("1")), List.of(1)));
        assertEquals(
                Map.of("1", 2, "3", 4, "5", 6),
                x.mapLiteral(POS, List.of(stringPos("1"), stringPos("3"), stringPos("5")), List.of(2, 4, 6)));
    }

    private ASTFactory.StringPos<ASTFactory.NULL> stringPos(String string) {
        return new ASTFactory.StringPos<>(string, POS);
    }
}
