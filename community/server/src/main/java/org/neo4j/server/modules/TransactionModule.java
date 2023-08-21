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
import org.neo4j.server.http.cypher.CypherResource;
import org.neo4j.server.http.cypher.format.input.json.JsonMessageBodyReader;
import org.neo4j.server.http.cypher.format.output.eventsource.LineDelimitedEventSourceJoltMessageBodyWriter;
import org.neo4j.server.http.cypher.format.output.eventsource.LineDelimitedEventSourceJoltV2MessageBodyWriter;
import org.neo4j.server.http.cypher.format.output.eventsource.SequentialEventSourceJoltMessageBodyWriter;
import org.neo4j.server.http.cypher.format.output.eventsource.SequentialEventSourceJoltV2MessageBodyWriter;
import org.neo4j.server.http.cypher.format.output.json.JsonMessageBodyWriter;
import org.neo4j.server.web.Injectable;
import org.neo4j.server.web.WebServer;
import org.neo4j.time.SystemNanoClock;

/**
 * Mounts the transactional endpoint.
 */
public class TransactionModule implements ServerModule {
    private final Config config;
    private final WebServer webServer;
    private final SystemNanoClock clock;

    public TransactionModule(WebServer webServer, Config config, SystemNanoClock clock) {
        this.webServer = webServer;
        this.config = config;
        this.clock = clock;
    }

    @Override
    public void start() {
        webServer.addJAXRSClasses(
                jaxRsClasses(), mountPoint(), List.of(Injectable.injectable(SystemNanoClock.class, clock)));
    }

    @Override
    public void stop() {
        webServer.removeJAXRSClasses(jaxRsClasses(), mountPoint());
    }

    private String mountPoint() {
        return config.get(ServerSettings.db_api_path).toString();
    }

    private static List<Class<?>> jaxRsClasses() {
        return List.of(
                CypherResource.class,
                JsonMessageBodyReader.class,
                JsonMessageBodyWriter.class,
                LineDelimitedEventSourceJoltMessageBodyWriter.class,
                SequentialEventSourceJoltMessageBodyWriter.class,
                LineDelimitedEventSourceJoltV2MessageBodyWriter.class,
                SequentialEventSourceJoltV2MessageBodyWriter.class);
    }
}
