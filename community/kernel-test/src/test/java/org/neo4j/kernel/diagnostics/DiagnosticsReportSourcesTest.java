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
package org.neo4j.kernel.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newFailedDiagnosticsSource;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class DiagnosticsReportSourcesTest {
    @Test
    void failedSourceKeepsTheDestinationItWouldHaveBeenWrittenTo() {
        DiagnosticsReportSource source = newFailedDiagnosticsSource("databases/neo4j/indexes.json", "boom");

        assertThat(source.destinationPath()).isEqualTo("databases/neo4j/indexes.json");
    }

    @Test
    void failedSourceThrowsTheCapturedErrorWhenRead() {
        String error = "Failed to run 'SHOW DATABASES YIELD *': connection refused";
        DiagnosticsReportSource source = newFailedDiagnosticsSource("databases.json", error);

        assertThatThrownBy(source::newInputStream)
                .isInstanceOf(IOException.class)
                .hasMessage(error);
    }

    @Test
    void failedSourceDoesNotContributeToTheDiskSpaceEstimate() {
        assertThat(newFailedDiagnosticsSource("databases.json", "boom").estimatedSize())
                .isZero();
    }

    @Test
    void failedSourceThrowsOnEveryRead() {
        DiagnosticsReportSource source = newFailedDiagnosticsSource("databases.json", "boom");

        assertThatThrownBy(source::newInputStream).isInstanceOf(IOException.class);
        assertThatThrownBy(source::newInputStream).isInstanceOf(IOException.class);
    }
}
