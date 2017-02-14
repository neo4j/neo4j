/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.rest;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.neo4j.server.NeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import java.util.concurrent.Callable;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public abstract class EnterpriseVersionIT extends ExclusiveServerTestBase {
    @ClassRule
    public static TemporaryFolder staticFolder = new TemporaryFolder();
    protected static NeoServer server;
    static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws Exception
    {
        FakeClock clock = Clocks.fakeClock();
        server = EnterpriseServerBuilder.server()
                .usingDataDir( staticFolder.getRoot().getAbsolutePath() )
                .withClock(clock)
                .build();

        suppressAll().call((Callable<Void>) () -> {
            server.start();
            return null;
        });
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @AfterClass
    public static void stopServer() throws Exception
    {
        suppressAll().call((Callable<Void>) () -> {
            server.stop();
            return null;
        });
    }

    @Before
    public void setupTheDatabase() throws Exception
    {
        // do nothing, we don't care about the database contents here
    }
}
