/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.modules;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.web.WebServer;

public class WebAdminModule implements ServerModule
{
    private static final Logger log = Logger.getLogger( WebAdminModule.class );

    private static final String DEFAULT_WEB_ADMIN_PATH = "/webadmin";
    private static final String DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION = "webadmin-html";

    private final WebServer webServer;

    public WebAdminModule( WebServer webServer )
    {
    	this.webServer = webServer;
    }

    @Override
	public void start(StringLogger logger)
    {
        webServer.addStaticContent( DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION, DEFAULT_WEB_ADMIN_PATH );

        log.info( "Mounted webadmin at [%s]", DEFAULT_WEB_ADMIN_PATH );
        if ( logger != null )
            logger.logMessage( "Mounted webadmin at: " + DEFAULT_WEB_ADMIN_PATH );
    }

    @Override
	public void stop()
    {
        webServer.removeStaticContent( DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION, DEFAULT_WEB_ADMIN_PATH );
    }
}
