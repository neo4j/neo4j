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
package org.neo4j.server.modules;

import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.RoundRobinJobScheduler;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.rrd.RrdFactory;
import org.rrd4j.core.RrdDb;

public class WebAdminModule implements ServerModule
{

    private static final Logger log = Logger.getLogger( WebAdminModule.class );

    private static final String DEFAULT_WEB_ADMIN_PATH = "/webadmin";
    private static final String DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION = "webadmin-html";

    private final RoundRobinJobScheduler jobScheduler = new RoundRobinJobScheduler();

    public void start( NeoServerWithEmbeddedWebServer neoServer )
    {
        try
        {
            startRoundRobinDB( neoServer );
        }
        catch ( RuntimeException e )
        {
            log.error( e );
            return;
        }
        neoServer.getWebServer()
                .addStaticContent( DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION, DEFAULT_WEB_ADMIN_PATH );
        log.info( "Mounted webadmin at [%s]", DEFAULT_WEB_ADMIN_PATH );
    }

    public void stop()
    {
        jobScheduler.stopJobs();
    }

    private void startRoundRobinDB( NeoServerWithEmbeddedWebServer neoServer )
    {
        Database db = neoServer.getDatabase();
        RrdFactory rrdFactory = new RrdFactory( neoServer.getConfiguration() );
        RrdDb rrdDb = rrdFactory.createRrdDbAndSampler( db, jobScheduler );
        db.setRrdDb( rrdDb );
    }
}
