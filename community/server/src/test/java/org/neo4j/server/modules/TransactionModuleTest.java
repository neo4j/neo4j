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
package org.neo4j.server.modules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.WebServer;

public class TransactionModuleTest {
    @SuppressWarnings("unchecked")
    @Test
    public void shouldRegisterASingleUri() {
        // Given
        WebServer webServer = mock(WebServer.class);

        Config config = Config.defaults(ServerSettings.db_api_path, URI.create("/db/data"));

        // When
        TransactionModule module = new TransactionModule(webServer, config, null);
        module.start();

        // Then
        ArgumentCaptor<List<Class<?>>> captor = ArgumentCaptor.forClass(List.class);
        verify(webServer).addJAXRSClasses(captor.capture(), anyString(), any());
        assertThat(captor.getValue()).isNotEmpty();
    }
}
