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

package org.neo4j.server.startup.healthcheck;

import java.util.Properties;

import org.neo4j.server.NeoServer;

public class ConfigFileMustBeSpecifiedAsASystemPropertyRule implements StartupHealthCheckRule {

    private boolean ran = false;
    private boolean passed = false;

    public boolean execute(Properties properties) {
        String key = properties.getProperty(NeoServer.NEO_CONFIG_FILE_PROPERTY);
        this.passed   = key != null;
        ran = true;
        return passed;
    }

    public String getMessage() {
        if(!ran) {
            return String.format("[%s] Healthcheck has not been run", this.getClass().getName());
        }
        
        if(passed) {
            return String.format("[%s] Passed healthcheck", this.getClass().getName());
        } else {
            return String.format("[%s] Failed healthcheck", this.getClass().getName());
        }
    }

}
