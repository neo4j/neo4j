/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.harness.internal;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import org.neo4j.harness.EnterpriseTestServerBuilders;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class EnterpriseInProcessServerBuilderIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldLaunchAServerInSpecifiedDirectory()
    {
        // Given
        File workDir = new File( testDir.directory(), "specific" );
        workDir.mkdir();

        // When
        try ( ServerControls server = getTestServerBuilder( workDir ).newServer() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( workDir.list().length, equalTo( 1 ) );
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( Arrays.toString( workDir.list() ), workDir.list().length, equalTo( 0 ) );
    }

    private TestServerBuilder getTestServerBuilder( File workDir )
    {
        String certificatesDirectoryKey = LegacySslPolicyConfig.certificates_directory.name();
        String certificatesDirectoryValue = ServerTestUtils.getRelativePath(
                testDir.directory(),
                LegacySslPolicyConfig.certificates_directory
        );

        return EnterpriseTestServerBuilders.newInProcessBuilder( workDir )
                .withConfig( certificatesDirectoryKey, certificatesDirectoryValue )
                .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .withConfig( ServerSettings.script_enabled, Settings.TRUE );
    }
}
