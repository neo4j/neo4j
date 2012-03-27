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
package org.neo4j.qa.clusterstate.modifier;

import org.neo4j.qa.clusterstate.Neo4jHAClusterDescription;
import org.neo4j.qa.clusterstate.ZookeeperClusterDescription;
import org.neo4j.qa.machinestate.MachineModel;
import org.neo4j.qa.machinestate.Neo4jHANodeDescription;
import org.neo4j.qa.machinestate.StateRegistry;
import org.neo4j.qa.machinestate.modifier.Neo4jHAInstallation;

public class Neo4jClusterInstallation implements ClusterModifier {

    public static Neo4jClusterInstallation neo4jClusterInstallation() {
        return new Neo4jClusterInstallation();
    }
    
    @Override
    public void modify(MachineModel[] machines, StateRegistry state)
    {
        ZookeeperClusterDescription zkCluster = state.get(ZookeeperClusterDescription.class);
        
        Neo4jHAClusterDescription cluster = createClusterDescription(machines);
        
        for(Neo4jHANodeDescription node : cluster.getNodes()) {
            node.getMachine().apply(new Neo4jHAInstallation(node, zkCluster));
        }
    }

    private Neo4jHAClusterDescription createClusterDescription(MachineModel[] machines)
    {
        Neo4jHAClusterDescription clusterDescription = new Neo4jHAClusterDescription();
        int id = 0;
        
        for(MachineModel machine : machines) {
            clusterDescription.addNode(new Neo4jHANodeDescription(++id, machine.getVMDefinition().ip(), machine));
        }
        return clusterDescription;
    }

}
