/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.apache.commons.lang.StringUtils;
import org.neo4j.server.NeoServer;
import org.neo4j.server.webdriver.WebDriverFacade;
import org.neo4j.server.webdriver.WebadminWebdriverLibrary;
import org.neo4j.test.server.ExclusiveServerTestBase;

public abstract class AbstractExclusiveServerWebadminTest extends ExclusiveServerTestBase {

    protected static WebadminWebdriverLibrary wl;
    
    private static WebDriverFacade webdriverFacade;

    public static void setupWebdriver(NeoServer server) throws Exception {
        webdriverFacade = new WebDriverFacade();
        wl = new WebadminWebdriverLibrary( webdriverFacade, deriveBaseUri(server) );
    }
    
    public static void shutdownWebdriver() throws Exception {
        webdriverFacade.quitBrowser();
    }

    private static String deriveBaseUri(NeoServer server)
    {
        String overrideBaseUri = System.getProperty( "webdriver.override.neo-server.baseuri" );
        if ( StringUtils.isNotEmpty( overrideBaseUri )) {
            return overrideBaseUri;
        }
        return server.baseUri().toString();
    }
}
