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
package org.neo4j.qa.machinestate.modifier;

import org.neo4j.qa.clusterstate.ZookeeperClusterDescription;
import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.machinestate.Neo4jHANodeDescription;
import org.neo4j.qa.machinestate.Neo4jInstallationState;
import org.neo4j.qa.machinestate.Neo4jServiceState;
import org.neo4j.qa.machinestate.StateAtom;
import org.neo4j.qa.machinestate.StateRegistry;

public class Neo4jHAInstallation implements MachineModifier {

    private Neo4jHANodeDescription description;
    private ZookeeperClusterDescription zkCluster;

    public Neo4jHAInstallation(Neo4jHANodeDescription description, ZookeeperClusterDescription zkCluster) {
        this.description = description;
        this.zkCluster = zkCluster;
    }
    
    @Override
    public void modify(Neo4jDriver driver, StateRegistry state)
    {

        String neo4jConf = driver.neo4jInstallDir() + "/conf/neo4j.properties";
        String serverConf = driver.neo4jInstallDir()
                + "/conf/neo4j-server.properties";
        String wrapperProperties = driver.neo4jInstallDir() + "/conf/neo4j-wrapper.conf";

        driver.installNeo4j();
        driver.stopNeo4j();

        driver.setConfig(neo4jConf, "ha.server_id", "" + description.getId());
        driver.setConfig(neo4jConf, "ha.server", description.getIp() + ":6001");
        driver.setConfig(neo4jConf, "ha.coordinators",
                zkCluster.getAdressString());

        driver.setConfig(serverConf, "org.neo4j.server.database.mode", "HA");
        driver.setConfig(serverConf, "org.neo4j.server.webserver.address",
                "0.0.0.0");

        driver.setConfig(wrapperProperties, "wrapper.java.maxmemory", "512");

        // The database folder has to be empty on first boot
        driver.deleteDatabase();

        driver.startNeo4j();
    }

    @Override
    public StateAtom[] stateModifications()
    {
        return new StateAtom[]{
                Neo4jInstallationState.INSTALLED,
                Neo4jServiceState.RUNNING};
    }
    
    @Override
	public String toString()
    {
    	return "Install neo4j enterprise";
    }

}
