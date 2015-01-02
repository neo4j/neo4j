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
package org.neo4j.server.modules;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;

import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.RoundRobinJobScheduler;
import org.neo4j.server.database.Database;
import org.neo4j.server.rrd.RrdFactory;
import org.neo4j.server.web.WebServer;

import org.rrd4j.core.RrdDb;

public class WebAdminModule implements ServerModule
{
    private static final String DEFAULT_WEB_ADMIN_PATH = "/webadmin";
    private static final String DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION = "webadmin-html";

    private final RoundRobinJobScheduler jobScheduler;

	private final Configuration config;
	private final WebServer webServer;
	private final Database database;

	private RrdDb rrdDb;
    private final ConsoleLogger log;
    private final Logging logging;

    public WebAdminModule(WebServer webServer, Configuration config, Logging logging, Database database)
    {
    	this.webServer = webServer;
    	this.config = config;
        this.logging = logging;
    	this.log = logging.getConsoleLog( getClass() );
    	this.database = database;
    	this.jobScheduler = new RoundRobinJobScheduler( logging );
    }

    @Override
	public void start()
    {
        try {
            startRoundRobinDB( );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        webServer.addStaticContent( DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION, DEFAULT_WEB_ADMIN_PATH );
        log.log( "Mounted webadmin at [%s]", DEFAULT_WEB_ADMIN_PATH );
    }

    @Override
	public void stop()
    {
    	jobScheduler.stopJobs();
    	webServer.removeStaticContent( DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION, DEFAULT_WEB_ADMIN_PATH );
        try {
        	if(rrdDb != null)
        	{
        		this.rrdDb.close();
        	}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

    private void startRoundRobinDB( ) throws IOException
    {
        RrdFactory rrdFactory = new RrdFactory( config, logging );
        this.rrdDb = rrdFactory.createRrdDbAndSampler( database, jobScheduler );
        database.setRrdDb( rrdDb );
    }
}
