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
package org.neo4j.qa.clusterstate.modifier;

import org.neo4j.qa.clusterstate.ZookeeperClusterDescription;
import org.neo4j.qa.machinestate.MachineModel;
import org.neo4j.qa.machinestate.StateRegistry;
import org.neo4j.qa.machinestate.ZookeeperNodeDescription;
import org.neo4j.qa.machinestate.modifier.ZookeeperInstallation;

public class ZookeeperClusterInstallation implements ClusterModifier {

    private static final int CLIENT_PORT = 2181;

    public static ZookeeperClusterInstallation zookeeperClusterInstallation() {
        return new ZookeeperClusterInstallation();
    }
    
    @Override
    public void modify(MachineModel[] machines, StateRegistry state)
    {
        ZookeeperClusterDescription clusterDescription = createClusterDescriptionFrom(machines);
        
        for(ZookeeperNodeDescription nodeDescription : clusterDescription.getNodes()) 
        {
            nodeDescription.getMachine().apply(new ZookeeperInstallation(nodeDescription, clusterDescription));
        }
        
        state.put(clusterDescription);
    }

    private ZookeeperClusterDescription createClusterDescriptionFrom(
            MachineModel[] machines)
    {
        ZookeeperClusterDescription clusterDescription = new ZookeeperClusterDescription();
        int id = 0;
        
        for(MachineModel machine : machines) {
            clusterDescription.addNode(new ZookeeperNodeDescription(++id, machine.getVMDefinition().ip(), CLIENT_PORT, machine));
        }
        return clusterDescription;
    }
    
    @Override
	public String toString()
    {
    	return "Install and configure zookeeper cluster";
    }
}
