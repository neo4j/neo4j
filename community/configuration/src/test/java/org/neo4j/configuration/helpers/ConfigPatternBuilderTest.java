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
package org.neo4j.configuration.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class ConfigPatternBuilderTest {
    @Test
    void shouldWorkWithoutWildcard() {
        checkMatchesAndEscapingWorks("foo", "foo");
        checkDoesNotMatchAndEscapingWorks(
                "foo", "", "Foo", "foO", "FOO", "fo", "fooo", " foo", "foo ", "fo ", "fo*", "fo?", "f00");
    }

    @Test
    void shouldWorkWithSingleQuestionMarkWildcard() {
        // Question Mark is supposed to match zero OR one character
        var single = "fo?";
        checkMatchesAndEscapingWorks(single, "fo", "foo", "fo1", "fo.", "fo ", "fo*", "fo?");
        checkDoesNotMatchAndEscapingWorks(single, "", "fooo", " foo", "foo ");
    }

    @Test
    void shouldWorkWithMultipleQuestionMarkWildcard() {
        var multipleIndependent = "?o?";
        checkMatchesAndEscapingWorks(multipleIndependent, "foo", "1o1", "o", "fo", "of", "o*", "?o");
        checkDoesNotMatchAndEscapingWorks(multipleIndependent, "", "fooo", " foo", "foo ", "afoo");
    }

    @Test
    void shouldWorkWithMultipleConsecutiveQuestionMarkWildcard() {
        var multipleConsecutive = "f??";
        checkMatchesAndEscapingWorks(multipleConsecutive, "foo", "fo1", "fo.", "fo ", "fo*", "fo?", "f  ", "f99");
        checkDoesNotMatchAndEscapingWorks(multipleConsecutive, "", "fooo", " foo", "foo ", "afoo");
    }

    @Test
    void shouldWorkWithAsteriskWildcard() {
        // Asterisk is supposed to match one OR more characters
        var single = "fo*";
        checkMatchesAndEscapingWorks(single, "foo", "fo1", "fo.", "fo ", "fo*", "fo?", "fooo", "foo ");
        checkDoesNotMatchAndEscapingWorks(single, "", "fo", " foo");
    }

    @Test
    void shouldWorkWithMultipleAsteriskWildcard() {

        var multipleIndependent = "*o*";
        checkMatchesAndEscapingWorks(multipleIndependent, "foo", "1o1", "fooo", "*o*", "?o?", " foo", "foo ", "afoo");
        checkDoesNotMatchAndEscapingWorks(multipleIndependent, "", "o", "fo", "of");
    }

    @Test
    void shouldWorkWithMultipleConsecutiveAsteriskWildcard() {
        var multipleConsecutive = "f**";
        checkMatchesAndEscapingWorks(
                multipleConsecutive, "foo", "fo1", "fooo", "foo ", "fo.", "fo ", "fo*", "fo?", "f  ", "f99");
        checkDoesNotMatchAndEscapingWorks(multipleConsecutive, "", "f", "fo", "afoo", " foo");
    }

    @Test
    void shouldWorkWithMultipleConsecutiveToAchieveMinimumCharsBehaviour() {
        var multipleConsecutive = "1*2**3***";
        checkMatchesAndEscapingWorks(
                multipleConsecutive, "1a2bb3ccc", "122223222", "1323333333", "1       2       3        4");
        checkDoesNotMatchAndEscapingWorks(multipleConsecutive, "", "123", "12 3  ", "123   3  2   23");
    }

    @Test
    void shouldWorkWithBothWildcards() {
        var multipleIndependent = "?o*";
        checkMatchesAndEscapingWorks(multipleIndependent, "o1", "foo", "1o1", "fooo", "*o*", "?o?", "foo ");
        checkDoesNotMatchAndEscapingWorks(multipleIndependent, "", "o", "fo", " foo", "afoo");
    }

    @Test
    void shouldWorkWithFlags() {
        assertMatches("foo", Pattern.CASE_INSENSITIVE, "foo", "FOO", "Foo", "foO");
        assertDoesNotMatch(
                "foo",
                Pattern.CASE_INSENSITIVE,
                "",
                "fo",
                "fooo",
                " foo",
                "foo ",
                "fo ",
                "foo\n",
                "\nfoo",
                "fo*",
                "fo?");

        assertMatches("foo*", Pattern.DOTALL, "foo\n", "foo \n", "fooo\n", "foo\n\n", "foo\no");
        assertDoesNotMatch("foo*", Pattern.DOTALL, "foo", "\nfooo", "fo\noo", "\nfoo\n", "boo\n");
    }

    // some strings that have special meaning in java regex
    private static final String[] unsupportedWildcards = new String[] {
        "[a-z]", ".", ".+", "+", "\\", "^", "&", "(a|b)", "[^1-9]", "\\d", "\\D", "\\s", "\\S", "\\w", "\\W"
    };

    // check that java regex special characters are escaped correctly
    private static void assertNotMessedUpByUnsupportedWildcards(
            String pattern, int flags, String match, boolean expectedMatch) {
        for (String wildcard : unsupportedWildcards) {
            if (expectedMatch) {
                assertMatches(pattern + wildcard, flags, match + wildcard);
                assertMatches(wildcard + pattern, flags, wildcard + pattern);
            } else {
                assertDoesNotMatch(pattern + wildcard, flags, match, match + wildcard, wildcard + match);
                assertDoesNotMatch(wildcard + pattern, flags, match, match + wildcard, wildcard + match);
            }
        }
    }

    private static void checkMatchesAndEscapingWorks(String pattern, String... expectedMatches) {
        assertMatches(pattern, 0, expectedMatches);

        for (String match : expectedMatches) {
            assertNotMessedUpByUnsupportedWildcards(pattern, 0, match, true);
        }
    }

    private static void assertMatches(String pattern, int flags, String... expectedMatches) {
        var p = ConfigPatternBuilder.patternFromConfigString(pattern, flags);
        assertThat(expectedMatches).allSatisfy(m -> assertThat(p.matcher(m).matches())
                .as(String.format("Pattern '%s' should match '%s' but does not. Java regex is %s", pattern, m, p))
                .isTrue());
    }

    private static void checkDoesNotMatchAndEscapingWorks(String pattern, String... expectedMatches) {
        assertDoesNotMatch(pattern, 0, expectedMatches);
        for (String match : expectedMatches) {
            assertNotMessedUpByUnsupportedWildcards(pattern, 0, match, false);
        }
    }

    private static void assertDoesNotMatch(String pattern, int flags, String... expectedMatches) {
        var p = ConfigPatternBuilder.patternFromConfigString(pattern, flags);
        assertThat(expectedMatches).allSatisfy(m -> assertThat(p.matcher(m).matches())
                .as(String.format("Pattern '%s' should NOT match '%s' but does. Java regex is %s", pattern, m, p))
                .isFalse());
    }
}
