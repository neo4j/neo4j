/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.configuration;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.server.configuration.ServerSettings.http_auth_allowlist;

@TestDirectoryExtension
class ServerSettingsMigratorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testWhitelistSettingsRename() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), List.of( "dbms.security.http_auth_whitelist=a,b" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThat( logProvider ).forClass( Config.class ).forLevel( WARN )
                .containsMessageWithArguments( "Use of deprecated setting %s. It is replaced by %s",
                        "dbms.security.http_auth_whitelist", http_auth_allowlist.name() );

        assertEquals( List.of( "a", "b"), config.get( http_auth_allowlist ) );
    }
}
