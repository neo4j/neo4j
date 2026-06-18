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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.string.Globbing;

class GlobbingPatternTest {
    @Test
    void invalidGlobbingPatternShouldThrow() {
        assertThatThrownBy(() -> new GlobbingPattern("invalid[globbing*pattern"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid globbing pattern 'invalid[globbing*pattern'");
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

        assertThat(globbingPattern.matches("pattern11.test")).isTrue();
        assertThat(globbingPattern.matches("patternstuff11.test")).isTrue();
        assertThat(globbingPattern.matches("pattern1.test")).isFalse();
        assertThat(globbingPattern.matches("pattern111test")).isFalse();
        assertThat(starsFirstLast.matches("pattern1.test")).isTrue();
        assertThat(starsFirstLast.matches("apattern1.testa")).isTrue();
        assertThat(questionMarks.matches("apattern1.testa")).isTrue();
        assertThat(questionMarks.matches("aapattern1.testaa")).isFalse();
        assertThat(questionMarks.matches("pattern1.test")).isFalse();
    }

    @Test
    void patternMatchingWithoutGlobbingCharsShouldWork() {
        GlobbingPattern empty = new GlobbingPattern("");
        GlobbingPattern space = new GlobbingPattern(" ");
        GlobbingPattern noGlobbing = new GlobbingPattern("full.name");
        GlobbingPattern all = new GlobbingPattern("*");

        assertThat(empty.matches("")).isTrue();
        assertThat(empty.matches(" ")).isFalse();
        assertThat(empty.matches("a")).isFalse();

        assertThat(space.matches("")).isFalse();
        assertThat(space.matches(" ")).isTrue();
        assertThat(space.matches("a")).isFalse();

        assertThat(all.matches("")).isTrue();
        assertThat(all.matches(" ")).isTrue();
        assertThat(all.matches("a")).isTrue();

        assertThat(noGlobbing.matches("full.name")).isTrue();
        assertThat(noGlobbing.matches("")).isFalse();
        assertThat(noGlobbing.matches("fullAname")).isFalse();
        assertThat(noGlobbing.matches("Afull.name")).isFalse();
        assertThat(noGlobbing.matches("full.nameA")).isFalse();
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
