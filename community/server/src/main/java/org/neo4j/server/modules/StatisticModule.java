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

import org.mortbay.jetty.Server;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.Database;
import org.neo4j.server.statistic.StatisticCollector;
import org.neo4j.server.statistic.StatisticFilter;
import org.neo4j.server.statistic.StatisticStartupListener;

public class StatisticModule implements ServerModule
{

    private StatisticStartupListener listener;

    public void start( NeoServerWithEmbeddedWebServer neoServer )
    {
        Server jetty = neoServer.getWebServer().getJetty();
        Database database = neoServer.getDatabase();

        //   ObjectName objectName = getObjectName( database.graph, Usage.NAME );

        StatisticCollector statisticCollector =
                neoServer.getDatabase().statisticCollector();

        //  MBeanServer mb = getPlatformMBeanServer();

        //   mb.createMBean( JmxUtils.getObjectName( database.graph,Usage.NAME ) )


        //      StatisticCollector statisticCollector =
        //              JmxUtils.getAttribute( objectName, "Collector" );

        listener = new StatisticStartupListener( jetty,
                new StatisticFilter( statisticCollector ) );
        jetty.addLifeCycleListener( listener );
    }

    public void stop()
    {
        listener.stop();
    }
}
