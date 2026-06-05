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

import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class ProcessorTest {

    private static String expectedIDENT(String s) {
        return "`" + s + "`";
    }

    private static String expectedSTRLIT(String s) {
        return "'" + s + "'";
    }

    private static String expectedUPPER(String s) {
        return s.toUpperCase(Locale.ROOT);
    }

    private static String expectedCALLABLE_IDENT(String s) {
        return s + "()";
    }

    GqlParams.ListProcessor processorNelistStrlit = new GqlParams.NELIST().withInner(new GqlParams.STRLIT());
    GqlParams.ListProcessor processorNelistCallableIdent =
            new GqlParams.NELIST().withInner(new GqlParams.CALLABLE_IDENT());
    GqlParams.ListProcessor processorNelistIdent = new GqlParams.NELIST().withInner(new GqlParams.IDENT());
    GqlParams.ListProcessor processorNelistVerbatim = new GqlParams.NELIST().withInner(new GqlParams.VERBATIM());
    GqlParams.Processor processorUpperCallableIdent = new GqlParams.UPPER().withInner(new GqlParams.CALLABLE_IDENT());
    GqlParams.Processor processorUpperIdent = new GqlParams.UPPER().withInner(new GqlParams.IDENT());
    GqlParams.Processor processorUpperStrlit = new GqlParams.UPPER().withInner(new GqlParams.STRLIT());
    GqlParams.Processor processorUpper = new GqlParams.UPPER();
    GqlParams.Processor processorParam = new GqlParams.PARAM();
    GqlParams.Processor processorIdent = new GqlParams.IDENT();
    GqlParams.Processor processorStrlit = new GqlParams.STRLIT();
    GqlParams.Processor processorCallableIdent = new GqlParams.CALLABLE_IDENT();

    // Fixing changes to format in these tests should be as easy as changing the corresponding
    // expectedPROCESSOR()-method
    @Test
    void testIDENT() {
        assertThat(processorIdent.process("abc")).isEqualTo(expectedIDENT("abc"));
        assertThat(processorIdent.process("abc()")).isEqualTo(expectedIDENT("abc()"));
        assertThat(processorIdent.process("{ %s } abc")).isEqualTo(expectedIDENT("{ %s } abc"));
        assertThat(processorIdent.process("`abc`")).isEqualTo(expectedIDENT("`abc`"));
    }

    @Test
    void testCALLABLE_IDENT() {
        assertThat(processorCallableIdent.process("abc")).isEqualTo(expectedCALLABLE_IDENT("abc"));
        assertThat(processorCallableIdent.process("abc()")).isEqualTo(expectedCALLABLE_IDENT("abc()"));
        assertThat(processorCallableIdent.process("{ %s } abc")).isEqualTo(expectedCALLABLE_IDENT("{ %s } abc"));
        assertThat(processorCallableIdent.process("'abc'")).isEqualTo(expectedCALLABLE_IDENT("'abc'"));
    }

    @Test
    void testSTRLIT() {
        assertThat(processorStrlit.process("abc")).isEqualTo(expectedSTRLIT("abc"));
        assertThat(processorStrlit.process("abc()")).isEqualTo(expectedSTRLIT("abc()"));
        assertThat(processorStrlit.process("{ %s } abc")).isEqualTo(expectedSTRLIT("{ %s } abc"));
        assertThat(processorStrlit.process("'abc'")).isEqualTo(expectedSTRLIT("'abc'"));
    }

    @Test
    void testPARAM() {
        assertThat(processorParam.process("abc")).isEqualTo("$" + expectedIDENT("abc"));
        assertThat(processorParam.process("abc()")).isEqualTo("$" + expectedIDENT("abc()"));
        assertThat(processorParam.process("{ %s } abc")).isEqualTo("$" + expectedIDENT("{ %s } abc"));
        assertThat(processorParam.process("'abc'")).isEqualTo("$" + expectedIDENT("'abc'"));
    }

    @Test
    void testUPPER() {
        assertThat(processorUpper.process("abc")).isEqualTo(expectedUPPER("abc"));
        assertThat(processorUpper.process("abc()")).isEqualTo(expectedUPPER("abc()"));
        assertThat(processorUpper.process("{ %s } abc")).isEqualTo(expectedUPPER("{ %s } abc"));
        assertThat(processorUpper.process("'abc'")).isEqualTo(expectedUPPER("'abc'"));
    }

    @Test
    void testUPPER_with_STRLIT() {
        assertThat(processorUpperStrlit.process("abc")).isEqualTo(expectedSTRLIT(expectedUPPER("abc")));
        assertThat(processorUpperStrlit.process("abc()")).isEqualTo(expectedSTRLIT(expectedUPPER("abc()")));
        assertThat(processorUpperStrlit.process("{ %s } abc")).isEqualTo(expectedSTRLIT(expectedUPPER("{ %s } abc")));
        assertThat(processorUpperStrlit.process("'abc'")).isEqualTo(expectedSTRLIT(expectedUPPER("'abc'")));
    }

    @Test
    void testUPPER_with_IDENT() {
        assertThat(processorUpperIdent.process("abc")).isEqualTo(expectedUPPER(expectedIDENT("abc")));
        assertThat(processorUpperIdent.process("abc()")).isEqualTo(expectedUPPER(expectedIDENT("abc()")));
        assertThat(processorUpperIdent.process("{ %s } abc")).isEqualTo(expectedUPPER(expectedIDENT("{ %s } abc")));
        assertThat(processorUpperIdent.process("'abc'")).isEqualTo(expectedUPPER(expectedIDENT("'abc'")));
    }

    @Test
    void testUPPER_with_CALLABLE_IDENT() {
        assertThat(processorUpperCallableIdent.process("abc")).isEqualTo(expectedUPPER(expectedCALLABLE_IDENT("abc")));
        assertThat(processorUpperCallableIdent.process("abc()"))
                .isEqualTo(expectedUPPER(expectedCALLABLE_IDENT("abc()")));
        assertThat(processorUpperCallableIdent.process("{ %s } abc"))
                .isEqualTo(expectedUPPER(expectedCALLABLE_IDENT("{ %s } abc")));
        assertThat(processorUpperCallableIdent.process("'abc'"))
                .isEqualTo(expectedUPPER(expectedCALLABLE_IDENT("'abc'")));
    }

    @Test
    void testNELIST_with_VERBATIM() {
        assertThat(processorNelistVerbatim.process(List.of("abc"), null)).isEqualTo("abc");
        assertThat(processorNelistVerbatim.process(List.of("abc()", "cbd()"), null))
                .isEqualTo("abc(), cbd()");
        assertThat(processorNelistVerbatim.process(List.of("{ %s } abc", "`abc`", "a"), null))
                .isEqualTo("{ %s } abc, `abc`, a");
    }

    @Test
    void testNELIST_with_IDENT() {
        assertThat(processorNelistIdent.process(List.of("abc"), null)).isEqualTo(expectedIDENT("abc"));
        assertThat(processorNelistIdent.process(List.of("abc()", "cbd()"), null))
                .isEqualTo(expectedIDENT("abc()") + ", " + expectedIDENT("cbd()"));
        assertThat(processorNelistIdent.process(List.of("{ %s } abc", "`abc`", "a"), null))
                .isEqualTo(expectedIDENT("{ %s } abc") + ", " + expectedIDENT("`abc`") + ", " + expectedIDENT("a"));
    }

    @Test
    void testNELIST_with_CALLABLE_IDENT() {
        assertThat(processorNelistCallableIdent.process(List.of("abc"), null)).isEqualTo(expectedCALLABLE_IDENT("abc"));
        assertThat(processorNelistCallableIdent.process(List.of("abc()", "cbd()"), null))
                .isEqualTo(expectedCALLABLE_IDENT("abc()") + ", " + expectedCALLABLE_IDENT("cbd()"));
        assertThat(processorNelistCallableIdent.process(List.of("{ %s } abc", "`abc`", "a"), null))
                .isEqualTo(expectedCALLABLE_IDENT("{ %s } abc") + ", " + expectedCALLABLE_IDENT("`abc`") + ", "
                        + expectedCALLABLE_IDENT("a"));
    }

    @Test
    void testNELIST_with_STRLIT() {
        assertThat(processorNelistStrlit.process(List.of("abc"), null)).isEqualTo(expectedSTRLIT("abc"));
        assertThat(processorNelistStrlit.process(List.of("abc()", "cbd()"), null))
                .isEqualTo(expectedSTRLIT("abc()") + ", " + expectedSTRLIT("cbd()"));
        assertThat(processorNelistStrlit.process(List.of("{ %s } abc", "`abc`", "a"), null))
                .isEqualTo(expectedSTRLIT("{ %s } abc") + ", " + expectedSTRLIT("`abc`") + ", " + expectedSTRLIT("a"));
    }

    @Test
    void testANDEDNELIST_with_VERBATIM() {
        var joinStyle = GqlParams.JoinStyle.ANDED;
        assertThat(processorNelistVerbatim.process(List.of("abc"), joinStyle)).isEqualTo("abc");
        assertThat(processorNelistVerbatim.process(List.of("abc()", "cbd()"), joinStyle))
                .isEqualTo("abc() and cbd()");
        assertThat(processorNelistVerbatim.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle))
                .isEqualTo("{ %s } abc, `abc` and a");
    }

    @Test
    void testANDEDNELIST_with_IDENT() {
        var joinStyle = GqlParams.JoinStyle.ANDED;
        assertThat(processorNelistIdent.process(List.of("abc"), joinStyle)).isEqualTo(expectedIDENT("abc"));
        assertThat(processorNelistIdent.process(List.of("abc()", "cbd()"), joinStyle))
                .isEqualTo(expectedIDENT("abc()") + " and " + expectedIDENT("cbd()"));
        assertThat(processorNelistIdent.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle))
                .isEqualTo(expectedIDENT("{ %s } abc") + ", " + expectedIDENT("`abc`") + " and " + expectedIDENT("a"));
    }

    @Test
    void testANDEDNELIST_with_CALLABLE_IDENT() {
        var joinStyle = GqlParams.JoinStyle.ANDED;
        assertThat(processorNelistCallableIdent.process(List.of("abc"), joinStyle))
                .isEqualTo(expectedCALLABLE_IDENT("abc"));
        assertThat(processorNelistCallableIdent.process(List.of("abc()", "cbd()"), joinStyle))
                .isEqualTo(expectedCALLABLE_IDENT("abc()") + " and " + expectedCALLABLE_IDENT("cbd()"));
        assertThat(processorNelistCallableIdent.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle))
                .isEqualTo(expectedCALLABLE_IDENT("{ %s } abc") + ", " + expectedCALLABLE_IDENT("`abc`") + " and "
                        + expectedCALLABLE_IDENT("a"));
    }

    @Test
    void testANDEDNELIST_with_STRLIT() {
        var joinStyle = GqlParams.JoinStyle.ANDED;

        assertThat(processorNelistStrlit.process(List.of("abc"), joinStyle)).isEqualTo(expectedSTRLIT("abc"));
        assertThat(processorNelistStrlit.process(List.of("abc()", "cbd()"), joinStyle))
                .isEqualTo(expectedSTRLIT("abc()") + " and " + expectedSTRLIT("cbd()"));
        assertThat(processorNelistStrlit.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle))
                .isEqualTo(
                        expectedSTRLIT("{ %s } abc") + ", " + expectedSTRLIT("`abc`") + " and " + expectedSTRLIT("a"));
    }

    // More readable, but tedious fixing of format-changes
    @Test
    void testLiteralMessages() {
        assertThat(processorNelistVerbatim.process(List.of(""), GqlParams.JoinStyle.ANDED))
                .isEqualTo("");
        assertThat(processorNelistVerbatim.process(List.of(""), GqlParams.JoinStyle.ORED))
                .isEqualTo("");
        assertThat(processorNelistVerbatim.process(List.of(""), GqlParams.JoinStyle.COMMAD))
                .isEqualTo("");
        assertThat(processorStrlit.process("abc")).isEqualTo("'abc'");
        assertThat(processorIdent.process("abc")).isEqualTo("`abc`");
        assertThat(processorParam.process("abc")).isEqualTo("$`abc`");
        assertThat(processorCallableIdent.process("abc")).isEqualTo("abc()");
        assertThat(processorNelistVerbatim.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.COMMAD))
                .isEqualTo("abc, def, ghi");
        assertThat(processorNelistVerbatim.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ORED))
                .isEqualTo("abc, def or ghi");
        assertThat(processorNelistVerbatim.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED))
                .isEqualTo("abc, def and ghi");
        assertThat(processorNelistCallableIdent.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED))
                .isEqualTo("abc(), def() and ghi()");
        assertThat(processorNelistIdent.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED))
                .isEqualTo("`abc`, `def` and `ghi`");
        assertThat(processorNelistStrlit.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED))
                .isEqualTo("'abc', 'def' and 'ghi'");
        assertThat(processorUpperCallableIdent.process("abc")).isEqualTo("ABC()");
        assertThat(processorUpperIdent.process("abc")).isEqualTo("`ABC`");
        assertThat(processorUpperStrlit.process("abc")).isEqualTo("'ABC'");
    }
}
