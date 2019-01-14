/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nonnull;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;

public class ServerBootstrapperTest
{
    @Rule
    public final SuppressOutput suppress = SuppressOutput.suppress( SuppressOutput.System.out );

    @Rule
    public TestDirectory homeDir = TestDirectory.testDirectory();

    @Test
    public void shouldNotThrowNullPointerExceptionIfConfigurationValidationFails() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = new ServerBootstrapper()
        {
            @Override
            protected NeoServer createNeoServer( Config config, GraphDatabaseDependencies dependencies,
                    LogProvider userLogProvider )
            {
                return mock( NeoServer.class );
            }

            @Nonnull
            @Override
            protected Collection<ConfigurationValidator> configurationValidators()
            {
                return Collections.emptyList();
            }
        };

        File dir = Files.createTempDirectory( "test-server-bootstrapper" ).toFile();
        dir.deleteOnExit();

        // when
        serverBootstrapper.start( dir, Optional.empty(), MapUtil.stringMap(
                database_path.name(), homeDir.absolutePath().getAbsolutePath() ) );

        // then no exceptions are thrown and
        assertThat( suppress.getOutputVoice().lines(), not( empty() ) );
    }
}
