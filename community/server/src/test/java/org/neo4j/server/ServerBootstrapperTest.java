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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.Files;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.database.CommunityGraphFactory;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class ServerBootstrapperTest
{
    @Inject
    private TestDirectory homeDir;
    @Inject
    private SuppressOutput suppress;

    @Test
    void shouldNotThrowNullPointerExceptionIfConfigurationValidationFails() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = new ServerBootstrapper()
        {
            @Override
            protected GraphFactory createGraphFactory( Config config )
            {
                return new CommunityGraphFactory();
            }

            @Override
            protected NeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseDependencies dependencies )
            {
                return mock( NeoServer.class );
            }
        };

        File dir = Files.createTempDirectory( "test-server-bootstrapper" ).toFile();
        dir.deleteOnExit();

        // when
        serverBootstrapper.start( dir, Optional.empty(), MapUtil.stringMap() );

        // then no exceptions are thrown and
        assertThat( suppress.getOutputVoice().lines(), not( empty() ) );
    }
}
