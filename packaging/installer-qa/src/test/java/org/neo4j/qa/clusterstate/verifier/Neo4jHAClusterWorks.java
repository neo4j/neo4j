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
package org.neo4j.qa.clusterstate.verifier;

import org.neo4j.qa.machinestate.MachineModel;
import org.neo4j.qa.machinestate.modifier.RestCreateNode;
import org.neo4j.qa.machinestate.verifier.RestDeleteNode;
import org.neo4j.qa.machinestate.verifier.RestNodeDoesntExist;
import org.neo4j.qa.machinestate.verifier.RestNodeExists;


public class Neo4jHAClusterWorks implements ClusterVerifier {

    public static Neo4jHAClusterWorks neo4jHAClusterWorks() {
        return new Neo4jHAClusterWorks();
    }

    @Override
    public void verify(MachineModel[] machines)
    {
        RestCreateNode createNode = new RestCreateNode();
        
        int rounds = 4;
        long nodeId = -1;
        for(int i=0; i < machines.length * rounds; i++) {
            MachineModel machine = machines[i % machines.length];
            try {
                switch(i % 4) {
                case 0:
                    machine.forceApply(createNode);
                    nodeId = createNode.getCreatedNodeId();
                    break;
                case 1:
                    machine.verifyThat(new RestNodeExists(nodeId));
                    break;
                case 2: 
                    machine.verifyThat(new RestNodeExists(nodeId));
                    machine.forceApply(new RestDeleteNode(nodeId));
                    break;
                case 3:
                    machine.verifyThat(new RestNodeDoesntExist(nodeId));
                    break;
                }
            } catch(Exception e) {
                throw new HAClusterDoesNotWorkException("Ensuring HA cluster works failed while executing request on server " 
                                           + machine.getVMDefinition().ip() + ". Was at stage "+i+" in loop, nodeId was "+nodeId+". See nested exception.", e);
            }
        }
    }
}
