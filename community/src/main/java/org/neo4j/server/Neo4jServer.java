/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server;

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.configuration.Configurator;

import java.io.File;
import java.util.logging.Logger;

/**
 * Application entry point for the Neo4j Server.
 */
public class Neo4jServer {
    private static Configurator configurator;

    private static Logger log = Logger.getLogger("neo4j-server");
    private static final String NEO_CONFIGDIR_PROPERTY = "neo-server.home";
    private static final String DEFAULT_NEO_CONFIGDIR = "etc";

    private Neo4jServer() {
    }

    public static void main(String[] args) {
        log.info("Starting Neo4j Server...");

        configurator = new Configurator(new File(getConfigDir()));
    }

    private static String getConfigDir() {
        return System.getProperty(NEO_CONFIGDIR_PROPERTY, DEFAULT_NEO_CONFIGDIR);
    }

    public static Configuration configuration() {
        return configurator.configuration();
    }
}
