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

import org.apache.commons.configuration.Configuration;

import org.neo4j.server.statistic.StatisticCollector;
import org.neo4j.server.statistic.StatisticFilter;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.configuration.Configurator.WEBSERVER_ENABLE_STATISTICS_COLLECTION;

public class StatisticModule implements ServerModule
{
    private final StatisticFilter filter;
	private final WebServer webServer;
    private final Configuration config;

    public StatisticModule(WebServer webServer, StatisticCollector requestStatistics, Configuration config)
    {
    	this.webServer = webServer;
        this.config = config;
        this.filter = new StatisticFilter( requestStatistics );
    }

    @Override
	public void start()
    {
        if (isStatisticsEnabled())
        {
            webServer.addFilter(filter, "/*");
        }
    }

    @Override
	public void stop()
    {
        if (isStatisticsEnabled())
        {
            webServer.removeFilter(filter, "/*");
        }
    }

    private boolean isStatisticsEnabled()
    {
        return config.getBoolean(WEBSERVER_ENABLE_STATISTICS_COLLECTION, false);
    }
}
