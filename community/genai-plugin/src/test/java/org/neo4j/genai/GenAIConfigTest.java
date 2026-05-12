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
package org.neo4j.genai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

class GenAIConfigTest {

    private GenAIConfig genAIConfig;
    private File genAIConfigFile;

    @BeforeEach
    void setup() throws Exception {
        InternalLogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.get(GraphDatabaseSettings.allow_file_urls)).thenReturn(false);

        genAIConfigFile =
                new File(getClass().getClassLoader().getResource("genai.conf").toURI());
        when(neo4jConfig.get(GraphDatabaseSettings.configuration_directory))
                .thenReturn(Path.of(genAIConfigFile.getParent()));

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        genAIConfig = new GenAIConfig(neo4jConfig, new SimpleLogService(logProvider), registry);
    }

    @Test
    void testDetermineNeo4jConfFolderDefault() {
        assertThat(genAIConfig.determineNeo4jConfFolder()).isEqualTo(genAIConfigFile.getParent());
    }

    @Test
    void testApocConfFileBeingLoaded() {
        genAIConfig.init();

        assertThat(genAIConfig.getStringProperty("foo")).isEqualTo("bar");
    }
}
