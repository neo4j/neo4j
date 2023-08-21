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
package org.neo4j.dbms.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;

import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
public class DefaultIdentityModuleTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldCreateServerIdAndReReadIt() throws IOException {
        // given
        var dataDir = testDirectory.file(DEFAULT_DATA_DIR_NAME);
        var layout = Neo4jLayout.of(testDirectory.homePath());
        var fs = testDirectory.getFileSystem();
        assertFalse(fs.fileExists(dataDir));
        var globalModule = mock(GlobalModule.class);
        when(globalModule.getNeo4jLayout()).thenReturn(layout);
        when(globalModule.getLogService()).thenReturn(NullLogService.getInstance());
        when(globalModule.getFileSystem()).thenReturn(fs);
        var uuid = UUID.randomUUID();

        // when
        var identityModule = new DefaultIdentityModule(globalModule, uuid);

        // then
        assertTrue(fs.fileExists(dataDir));
        assertTrue(fs.fileExists(layout.serverIdFile()));
        assertNotNull(identityModule.serverId());
        assertEquals(uuid, identityModule.serverId().uuid());

        // when
        var secondIdentityModule = new DefaultIdentityModule(globalModule, UUID.randomUUID());

        // then
        assertEquals(identityModule.serverId(), secondIdentityModule.serverId());

        fs.deleteRecursively(dataDir);
        var thirdIdentityModule = new DefaultIdentityModule(globalModule, UUID.randomUUID());

        // then
        assertNotEquals(secondIdentityModule.serverId(), thirdIdentityModule.serverId());
    }
}
