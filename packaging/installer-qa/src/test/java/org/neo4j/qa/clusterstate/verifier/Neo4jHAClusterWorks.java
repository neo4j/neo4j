/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.qa.machinestate.verifier.HABeanExists;
import org.neo4j.qa.machinestate.verifier.RestDeleteNode;
import org.neo4j.qa.machinestate.verifier.RestNodeDoesntExist;
import org.neo4j.qa.machinestate.verifier.RestNodeExists;


public class Neo4jHAClusterWorks implements ClusterVerifier {

	private enum State
	{
		CREATE_NODE,
		VERIFY_NODE_EXISTS,
		DELETE_NODE,
		VERIFY_NODE_DELETED
	}
	
    public static Neo4jHAClusterWorks neo4jHAClusterWorks() {
        return new Neo4jHAClusterWorks();
    }

    @Override
    public void verify(MachineModel[] machines)
    {
    	for(MachineModel machine : machines)
    	{
    		machine.verifyThat(new HABeanExists(machines));
    	}
    	
        RestCreateNode createNode = new RestCreateNode();
        
        int rounds = 4;
        long nodeId = -1;
        for(int i=0; i < machines.length * rounds; i++) {
            MachineModel machine = machines[i % machines.length];
            try {
                switch(pickState(i)) {
                case CREATE_NODE:
                    machine.forceApply(createNode);
                    nodeId = createNode.getCreatedNodeId();
                    break;
                case VERIFY_NODE_EXISTS:
                    machine.verifyThat(new RestNodeExists(nodeId));
                    break;
                case DELETE_NODE: 
                    machine.verifyThat(new RestNodeExists(nodeId));
                    machine.forceApply(new RestDeleteNode(nodeId));
                    break;
                case VERIFY_NODE_DELETED:
                    machine.verifyThat(new RestNodeDoesntExist(nodeId));
                    break;
                }
            } catch(Exception e) {
            	switch(pickState(i)) {
                case CREATE_NODE:
                	throw new HAClusterDoesNotWorkException(
                			"Creating a node on server '" 
                            + machine.getVMDefinition().ip() + "' in the cluster failed. See nested exception.", e);
                case VERIFY_NODE_EXISTS:
                	throw new HAClusterDoesNotWorkException(
                			"Expected node with id "+nodeId+" that I had created on separate server to appear on server '" 
                            + machine.getVMDefinition().ip() + "',  but failed. See nested exception.", e);
                case DELETE_NODE: 
                	throw new HAClusterDoesNotWorkException(
                			"Deleting node with id "+nodeId+", which I had created on separate server, failed on server '" 
                            + machine.getVMDefinition().ip() + "'. See nested exception.", e);
                case VERIFY_NODE_DELETED:
                	throw new HAClusterDoesNotWorkException(
                			"Expected node with id "+nodeId+", which I had deleted on separate server, to disappear on server '" 
                            + machine.getVMDefinition().ip() + "', but it it didn't. See nested exception.", e);
                }
                throw new HAClusterDoesNotWorkException("Ensuring HA cluster works failed while executing request on server " 
                                           + machine.getVMDefinition().ip() + ". Was at stage "+i+" in loop, nodeId was "+nodeId+". See nested exception.", e);
            }
        }
    }

	private State pickState(int i) {
		switch(i % 4)
		{
		case 0: return State.CREATE_NODE;
        case 1: return State.VERIFY_NODE_EXISTS;
        case 2: return State.DELETE_NODE;
        case 3: return State.VERIFY_NODE_DELETED;
        default:
        	throw new RuntimeException("Unknown state.");
		}
	}
    
    @Override
	public String toString()
    {
    	return "That basic CRUD REST operations work in the cluster, and propagate to all nodes";
    }
    
    
}
