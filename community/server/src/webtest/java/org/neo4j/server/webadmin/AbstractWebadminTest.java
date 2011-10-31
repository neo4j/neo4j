/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.webadmin;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.webdriver.WebDriverFacade;
import org.neo4j.server.webdriver.WebadminWebdriverLibrary;

public abstract class AbstractWebadminTest {
    
    protected static WebadminWebdriverLibrary wl;
    
    private static NeoServerWithEmbeddedWebServer server;
    private static WebDriverFacade webdriverFacade;

    @BeforeClass
    public static void setup() throws Exception {
        server = ServerBuilder.server().build();
        server.start();
        
        webdriverFacade = new WebDriverFacade();
        wl = new WebadminWebdriverLibrary(webdriverFacade,server.baseUri().toString());
    }
    
    @Before
    public void cleanDatabase() {
        ServerHelper.cleanTheDatabase(server);
    }
    
    @AfterClass
    public static void tearDown() throws Exception {
        webdriverFacade.closeBrowser();
        server.stop();
    }
    
}
