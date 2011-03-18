/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.modules;

import org.neo4j.server.JAXRSHelper;
import org.neo4j.server.NeoServer;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.RoundRobinJobScheduler;
import org.neo4j.server.database.Database;
import org.neo4j.server.ext.visualization.VisualizationServlet;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.rrd.RrdFactory;
import org.rrd4j.core.RrdDb;

import javax.management.MalformedObjectNameException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class WebAdminModule implements ServerModule
{

    private static final Logger log = Logger.getLogger( WebAdminModule.class );

    private static final String DEFAULT_WEB_ADMIN_PATH = "/webadmin";
    private static final String DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION = "webadmin-html";

    private static final String DEFAULT_WEB_VISUALIZATION_PATH = "/visualization";

    private NeoServer neoServer;
    private final RoundRobinJobScheduler jobScheduler = new RoundRobinJobScheduler();

    public Set<URI> start( NeoServerWithEmbeddedWebServer neoServer )
    {
        this.neoServer = neoServer;
        try
        {
            startRoundRobinDB();
        } catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
        neoServer.getWebServer().addStaticContent( DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION, DEFAULT_WEB_ADMIN_PATH );
        log.info( "Mounted webadmin at [%s]", DEFAULT_WEB_ADMIN_PATH );

        HashSet<URI> ownedUris = new HashSet<URI>();
<<<<<<< HEAD
        ownedUris.add(JAXRSHelper.generateUriFor(neoServer.baseUri(), DEFAULT_WEB_ADMIN_PATH));

        neoServer.getWebServer().addServlet(new VisualizationServlet(neoServer.getDatabase().graph), DEFAULT_WEB_VISUALIZATION_PATH);
        log.info("Mounted visualization at [%s]", DEFAULT_WEB_VISUALIZATION_PATH);
=======
        ownedUris.add( JAXRSHelper.generateUriFor( neoServer.baseUri(), DEFAULT_WEB_ADMIN_PATH ) );
>>>>>>> 65b608e66bdffbdf64db3cf36ce44eed8f5efa4a

        return ownedUris;
    }

    public void stop()
    {
        jobScheduler.stopJobs();
    }

    private void startRoundRobinDB() throws MalformedObjectNameException, IOException
    {
        Database db = neoServer.getDatabase();
        RrdFactory rrdFactory = new RrdFactory( neoServer.getConfiguration() );
        RrdDb rrdDb = rrdFactory.createRrdDbAndSampler( db.graph, jobScheduler );
        db.setRrdDb( rrdDb );
    }
}
