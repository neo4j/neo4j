/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.modules;

import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.QueryResource;
import org.neo4j.server.queryapi.metrics.QueryAPIMetricsFilter;
import org.neo4j.server.queryapi.metrics.QueryAPIMetricsMonitor;
import org.neo4j.server.queryapi.request.JsonMessageBodyReader;
import org.neo4j.server.queryapi.request.TypedJsonMessageBodyReader;
import org.neo4j.server.queryapi.response.PlainJsonDriverResultWriter;
import org.neo4j.server.queryapi.response.TypedJsonDriverResultWriter;
import org.neo4j.server.web.WebServer;

/**
 * Mounts the Query API
 */
public class QueryModule implements ServerModule {

    private final WebServer webServer;
    private final Config config;

    private final QueryAPIMetricsMonitor metricsMonitor;

    public QueryModule(WebServer webServer, Config config, QueryAPIMetricsMonitor metricsMonitor) {
        this.webServer = webServer;
        this.config = config;
        this.metricsMonitor = metricsMonitor;
    }

    @Override
    public void start() {
        webServer.addJAXRSClasses(
                jaxRsClasses(), config.get(ServerSettings.db_api_path).toString(), null);
        webServer.addFilter(
                new QueryAPIMetricsFilter(
                        metricsMonitor, config.get(ServerSettings.db_api_path).toString()),
                "/*");
    }

    @Override
    public void stop() {
        webServer.removeJAXRSClasses(
                jaxRsClasses(), config.get(ServerSettings.db_api_path).toString());
    }

    private static List<Class<?>> jaxRsClasses() {
        return List.of(
                QueryResource.class,
                PlainJsonDriverResultWriter.class,
                TypedJsonDriverResultWriter.class,
                JsonMessageBodyReader.class,
                TypedJsonMessageBodyReader.class);
    }
}
