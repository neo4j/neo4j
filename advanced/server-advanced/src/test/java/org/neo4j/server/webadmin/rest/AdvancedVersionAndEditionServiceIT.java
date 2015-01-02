/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.webadmin.rest;

import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.KernelData;
import org.neo4j.server.NeoServer;
import org.neo4j.server.advanced.helpers.AdvancedServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.Mute.muteAll;

/*
Note that when running this test from within an IDE, the version field will be an empty string. This is because the
code that generates the version identifier is written by Maven as part of the build process(!). The tests will pass
both in the IDE (where the empty string will be correctly compared).
 */
public class AdvancedVersionAndEditionServiceIT extends ExclusiveServerTestBase
{
    private static NeoServer server;
    private static FunctionalTestHelper functionalTestHelper;

    @ClassRule
    public static TemporaryFolder staticFolder = new TemporaryFolder();

    private static FakeClock clock;

    @BeforeClass
    public static void setupServer() throws Exception
    {
        clock = new FakeClock();
        server = AdvancedServerBuilder.server()
                .usingDatabaseDir( staticFolder.getRoot().getAbsolutePath() )
                .withClock( clock )
                .build();

        muteAll().call( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                AdvancedVersionAndEditionServiceIT.server.start();
                return null;
            }
        } );
        functionalTestHelper = new FunctionalTestHelper( AdvancedVersionAndEditionServiceIT.server );
    }

    @Before
    public void setupTheDatabase() throws Exception
    {
        // do nothing, we don't care about the database contents here
    }

    @AfterClass
    public static void stopServer() throws Exception
    {
        muteAll().call( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                server.stop();
                return null;
            }
        } );
    }

    @Test
    public void shouldReportAdvancedEdition() throws Exception
    {
        String releaseVersion = server.getDatabase().getGraph().getDependencyResolver().resolveDependency( KernelData
                .class ).version().getReleaseVersion();
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.managementUri() + "/" +
                VersionAndEditionService.SERVER_PATH );

        assertEquals( 200, response.getStatus() );
        assertThat( response.getEntity(), containsString( "edition: \"advanced\"" ) );
        assertThat( response.getEntity(), containsString( "version: \"" + releaseVersion + "\"" ) );
    }
}