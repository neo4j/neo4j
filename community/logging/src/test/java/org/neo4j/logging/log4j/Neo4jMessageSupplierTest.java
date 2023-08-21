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
package org.neo4j.logging.log4j;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.logging.log4j.LogConfigTest.DATE_PATTERN;

import java.io.ByteArrayOutputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.logging.Level;
import org.neo4j.logging.Neo4jMessageSupplier;

class Neo4jMessageSupplierTest extends Log4jLogTestBase {
    @ParameterizedTest(name = "{1}")
    @MethodSource("logMethods")
    void shouldSupplyMessageWithFormat(LogMethod logMethod, Level level) {
        logMethod.log(log, () -> Neo4jMessageSupplier.forMessage("my %s message %d", "long", 1));

        assertThat(outContent.toString())
                .matches(format(DATE_PATTERN + " %-5s \\[className\\] my long message 1%n", level));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("logMethods")
    void shouldOnlyEvaluateArgWhenNeeded(LogMethod logMethod, Level level) {
        for (Level configuredLevel : Level.values()) {
            ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            try (Neo4jLoggerContext context = LogConfig.createBuilderToOutputStream(outContent, configuredLevel)
                    .build()) {
                Log4jLog log = new Log4jLog(context.getLogger("className"));

                Neo4jMessageSupplier supplier = mock(Neo4jMessageSupplier.class);
                doReturn(Neo4jMessageSupplier.forMessage("my arg %s", "foo"))
                        .when(supplier)
                        .get();
                logMethod.log(log, supplier);

                if (level.compareTo(configuredLevel) < 0) {
                    verifyNoInteractions(supplier);
                    assertThat(outContent.toString()).isEmpty();
                } else {
                    verify(supplier, times(1)).get();
                    verifyNoMoreInteractions(supplier);
                    assertThat(outContent.toString())
                            .matches(format(DATE_PATTERN + " %-5s \\[className\\] my arg foo%n", level));
                }
            }
        }
    }
}
