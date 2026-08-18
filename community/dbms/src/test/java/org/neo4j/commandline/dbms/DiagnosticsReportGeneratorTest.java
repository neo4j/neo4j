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
package org.neo4j.commandline.dbms;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.diagnostics.DiagnosticsReportInfo;
import org.neo4j.kernel.diagnostics.DiagnosticsReportManifest;

class DiagnosticsReportGeneratorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void archiveFileNameAndManifestAreDerivedFromTheSameIdentity() throws Exception {
        DiagnosticsReportInfo info =
                new DiagnosticsReportInfo("myhost", OffsetDateTime.of(2026, 7, 23, 14, 15, 30, 0, ZoneOffset.UTC));

        String fileName = DiagnosticsReportGenerator.getFilename(info);
        JsonNode manifest =
                MAPPER.readTree(new DiagnosticsReportManifest(info.hostName(), info.timestamp(), List.of()).toJson());

        assertThat(fileName).isEqualTo("myhost-2026-07-23_141530.zip");
        assertThat(manifest.get("hostname").asText()).isEqualTo("myhost");
        assertThat(manifest.get("timestamp").asText()).isEqualTo("2026-07-23T14:15:30Z");
    }

    @Test
    void fileNameSanitizesHostNameThatTheManifestKeepsVerbatim() throws Exception {
        DiagnosticsReportInfo info =
                new DiagnosticsReportInfo("host name/1", OffsetDateTime.of(2026, 7, 23, 14, 15, 30, 0, ZoneOffset.UTC));

        assertThat(DiagnosticsReportGenerator.getFilename(info)).isEqualTo("host_name_1-2026-07-23_141530.zip");
        JsonNode manifest =
                MAPPER.readTree(new DiagnosticsReportManifest(info.hostName(), info.timestamp(), List.of()).toJson());
        assertThat(manifest.get("hostname").asText()).isEqualTo("host name/1");
    }
}
