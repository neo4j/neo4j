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
import static org.neo4j.gqlstatus.SimpleMessageFormat.compile;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

// This test lives in this module to avoid circular dependency of RandomExtension.
@ExtendWith(RandomExtension.class)
public class SimpleMessageFormatPropertyTest {
    @Inject
    private RandomSupport rand;

    @Test
    void substituteArbitraryStrings() {
        for (int i = 0; i < 100; i++) {
            final var parts = Stream.generate(this::randomStringWithoutSubstitution)
                    .limit(rand.nextInt(100))
                    .toList();
            final var template = String.join("%s", parts);
            final var substitutionCount = Math.max(0, parts.size() - 1);
            final var messageFormat = compile(template);

            assertSubstitution(messageFormat, parts, randomStrings(substitutionCount));

            // MessageFormat can be re-used
            assertSubstitution(messageFormat, parts, randomStrings(substitutionCount));

            // Handles wrong number of parameters
            assertSubstitution(messageFormat, parts, randomStrings(rand.nextInt(1 + substitutionCount * 2)));
        }
    }

    private void assertSubstitution(SimpleMessageFormat messageFormat, List<String> parts, Object[] substitutions) {
        final var expectedBuilder = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            expectedBuilder.append(parts.get(i));
            if (i < parts.size() - 1) expectedBuilder.append(i < substitutions.length ? substitutions[i] : "null");
        }
        final var expected = expectedBuilder.toString();

        assertThat(messageFormat.format(substitutions)).isEqualTo(expected);

        final var prefix = rand.nextString();
        assertThat(messageFormat.format(new StringBuilder(prefix), substitutions))
                .asString()
                .isEqualTo(prefix + expected);
    }

    private String randomStringWithoutSubstitution() {
        String withoutSubstitution = rand.randomValues().nextTextValue(0, 12).stringValue();
        while (withoutSubstitution.contains("%s")) withoutSubstitution = withoutSubstitution.replace("%s", "");
        return withoutSubstitution;
    }

    private Object[] randomStrings(int size) {
        return Stream.generate(() -> rand.randomValues().nextTextValue(0, 24).stringValue())
                .limit(size)
                .toArray();
    }
}
