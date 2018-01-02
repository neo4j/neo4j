/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.junit.Assert.assertEquals;

public class DatabaseTuningDocIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @Test
    public void shouldLoadAKnownGoodPropertyFile() throws IOException
    {
        // given
        server = CommunityServerBuilder.server()
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withDefaultDatabaseTuning()
                .build();

        // when
        server.start();

        // then
        Map<String,String> params = server.getConfig().getParams();

        for ( Map.Entry<String, String> entry : CommunityServerBuilder.good_tuning_file_properties.entrySet() )
        {
            assertEquals( entry.getValue(), params.get( entry.getKey() ) );
        }
    }

    @Test
    public void shouldLogWarningAndContinueIfTuningFilePropertyDoesNotResolve() throws IOException
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        server = CommunityServerBuilder.server( logProvider )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withNonResolvableTuningFile()
                .build();

        // when
        server.start();

        // then
        logProvider.assertContainsMessageContaining( "Config file [%s] does not exist." );
    }

    @After
    public void cleanup()
    {
        if( server != null )
        {
            server.stop();
        }
    }
}
