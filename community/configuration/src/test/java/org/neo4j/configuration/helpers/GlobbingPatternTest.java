/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

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

        assertTrue(empty.matches(""));
        assertFalse(empty.matches(" "));
        assertFalse(empty.matches("a"));

        assertFalse(space.matches(""));
        assertTrue(space.matches(" "));
        assertFalse(space.matches("a"));

        assertTrue(noGlobbing.matches("full.name"));
        assertFalse(noGlobbing.matches(""));
        assertFalse(noGlobbing.matches("fullAname"));
        assertFalse(noGlobbing.matches("Afull.name"));
        assertFalse(noGlobbing.matches("full.nameA"));
    }
}
