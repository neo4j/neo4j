/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.server.statistic.StatisticCollector;
import org.neo4j.server.statistic.StatisticFilter;
import org.neo4j.server.web.WebServer;

public class StatisticModule implements ServerModule
{
    private final StatisticFilter filter;
	private final WebServer webServer;
    
    public StatisticModule(WebServer webServer, StatisticCollector requestStatistics)
    {
    	this.webServer = webServer;
    	this.filter = new StatisticFilter( requestStatistics );
    }

	@Override
	public void start(StringLogger logger)
    {
        webServer.addFilter(filter, "/*");
    }

    @Override
	public void stop()
    {
    	webServer.removeFilter(filter, "/*");
    }
}
