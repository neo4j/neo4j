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

import static org.neo4j.qa.machinestate.modifier.Neo4jServiceStateModifier.neo4jStartCommand;
import static org.neo4j.qa.machinestate.modifier.Neo4jServiceStateModifier.neo4jStopCommand;

import org.neo4j.qa.clusterstate.ZookeeperClusterDescription;
import org.neo4j.qa.machinestate.MachineModel;
import org.neo4j.qa.machinestate.StateRegistry;
import org.neo4j.qa.machinestate.modifier.DestroyNeo4jData;
import org.neo4j.qa.machinestate.modifier.FullHABackup;
import org.neo4j.qa.machinestate.modifier.IncrementalHABackup;
import org.neo4j.qa.machinestate.modifier.ReplaceDataWithBackup;
import org.neo4j.qa.machinestate.modifier.RestCreateNode;
import org.neo4j.qa.machinestate.verifier.RestNodeExists;

public class Neo4jHABackup implements ClusterModifier {
    
    public static Neo4jHABackup neo4jHABackup() {
        return new Neo4jHABackup();
    }

    @Override
    public void modify(MachineModel[] machines, StateRegistry state)
    {
        RestCreateNode createNode = new RestCreateNode();
        ZookeeperClusterDescription zkCluster = state.get(ZookeeperClusterDescription.class);
        
        MachineModel backupMachine = machines[0];
        backupMachine.apply(createNode);
        long nodeId = createNode.getCreatedNodeId();
        
        backupMachine.forceApply(new FullHABackup("test", zkCluster));
        backupMachine.forceApply(new IncrementalHABackup("test", zkCluster));
        
        for(MachineModel machine : machines) {
            machine.forceApply(neo4jStopCommand());
            machine.forceApply(new DestroyNeo4jData());
        }
        
        backupMachine.forceApply(new ReplaceDataWithBackup("test"));
        
        backupMachine.forceApply(neo4jStartCommand());
        backupMachine.verifyThat(new RestNodeExists(nodeId));
        
        for(MachineModel machine : machines) {
            machine.apply(neo4jStartCommand());
            machine.verifyThat(new RestNodeExists(nodeId));
        }
    }
}
