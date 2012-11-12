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
package org.neo4j.qa.clusterstate;

import org.neo4j.qa.clusterstate.modifier.ClusterModifier;
import org.neo4j.qa.clusterstate.verifier.ClusterVerifier;
import org.neo4j.qa.machinestate.MachineModel;
import org.neo4j.qa.machinestate.StateRegistry;

public class DefaultMachineClusterModel implements MachineClusterModel {

    private MachineModel[] machines;
    private StateRegistry state = new StateRegistry();

    public DefaultMachineClusterModel(MachineModel ... machines)
    {
        this.machines = machines; 
    }

    @Override
    public void forceApply(ClusterModifier modifier)
    {
    	System.out.println("[Cluster] Force applying: '" + modifier + "' (" + modifier.getClass().getSimpleName() + ")");
        modifier.modify(machines, state);
    }

    @Override
    public void apply(ClusterModifier modifier)
    {
    	System.out.println("[Cluster] Applying: '" + modifier + "' (" + modifier.getClass().getSimpleName() + ")");
        modifier.modify(machines, state);
    }

    @Override
    public void verifyThat(ClusterVerifier verifier)
    {
    	System.out.println("[Cluster] Verifying: '" + verifier + "' (" + verifier.getClass().getSimpleName() + ")");
        verifier.verify(machines);
    }

}
