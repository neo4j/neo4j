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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.string.Globbing;

class GlobbingPatternTest {
    @Test
    void invalidGlobbingPatternShouldThrow() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new GlobbingPattern("invalid[globbing*pattern"));
        assertThat(exception.getMessage()).isEqualTo("Invalid globbing pattern 'invalid[globbing*pattern'");
    }

    @Test
    void createShouldBeAbleToCreateMultiplePatterns() {
        List<GlobbingPattern> globbingPatterns = GlobbingPattern.create("*pattern1", "pattern?2");

        assertThat(globbingPatterns)
                .containsExactly(new GlobbingPattern("*pattern1"), new GlobbingPattern("pattern?2"));
    }

    @Test
    void patternMatchingWithGlobbingCharsShouldWork() {
        GlobbingPattern globbingPattern = new GlobbingPattern("pattern*1?.test");
        GlobbingPattern starsFirstLast = new GlobbingPattern("*pattern1.test*");
        GlobbingPattern questionMarks = new GlobbingPattern("?pattern1.test?");

        assertTrue(globbingPattern.matches("pattern11.test"));
        assertTrue(globbingPattern.matches("patternstuff11.test"));
        assertFalse(globbingPattern.matches("pattern1.test"));
        assertFalse(globbingPattern.matches("pattern111test"));
        assertTrue(starsFirstLast.matches("pattern1.test"));
        assertTrue(starsFirstLast.matches("apattern1.testa"));
        assertTrue(questionMarks.matches("apattern1.testa"));
        assertFalse(questionMarks.matches("aapattern1.testaa"));
        assertFalse(questionMarks.matches("pattern1.test"));
    }

    @Test
    void patternMatchingWithoutGlobbingCharsShouldWork() {
        GlobbingPattern empty = new GlobbingPattern("");
        GlobbingPattern space = new GlobbingPattern(" ");
        GlobbingPattern noGlobbing = new GlobbingPattern("full.name");
        GlobbingPattern all = new GlobbingPattern("*");

        assertTrue(empty.matches(""));
        assertFalse(empty.matches(" "));
        assertFalse(empty.matches("a"));

        assertFalse(space.matches(""));
        assertTrue(space.matches(" "));
        assertFalse(space.matches("a"));

        assertTrue(all.matches(""));
        assertTrue(all.matches(" "));
        assertTrue(all.matches("a"));

        assertTrue(noGlobbing.matches("full.name"));
        assertFalse(noGlobbing.matches(""));
        assertFalse(noGlobbing.matches("fullAname"));
        assertFalse(noGlobbing.matches("Afull.name"));
        assertFalse(noGlobbing.matches("full.nameA"));
    }

    private record Combination(List<String> include, List<String> exclude, List<String> expected) {
        static final List<String> INPUTS =
                List.of("", " ", "a", "A", "fulla", "fullA", "something.a", "something.A", "b", "fullb");
    }

    private static Stream<Combination> combinations() {
        return Stream.of(
                new Combination(List.of(), List.of(), List.of()), // at least one include must be matched
                new Combination(List.of("*"), List.of(), Combination.INPUTS),
                new Combination(List.of("*"), List.of("*"), List.of()), // exclude takes precedence
                new Combination(
                        List.of("*a"), List.of(), List.of("a", "A", "fulla", "fullA", "something.a", "something.A")),
                new Combination(List.of("*a"), List.of("fulla"), List.of("a", "A", "something.a", "something.A")),
                new Combination(List.of("*b"), List.of("*a"), List.of("b", "fullb")));
    }

    @ParameterizedTest
    @MethodSource("combinations")
    void testCompose(Combination combination) {
        var predicate = Globbing.compose(combination.include(), combination.exclude());
        var actual = Combination.INPUTS.stream().filter(predicate).toList();
        assertThat(actual).isEqualTo(combination.expected());
    }
}
