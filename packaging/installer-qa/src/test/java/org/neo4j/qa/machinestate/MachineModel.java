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
package org.neo4j.qa.machinestate;

import org.neo4j.qa.machinestate.modifier.MachineModifier;
import org.neo4j.qa.machinestate.verifier.Verifier;
import org.neo4j.vagrant.VMDefinition;

public interface MachineModel {
    
    /**
     * Apply a state modification to this machine.
     * @param modifier
     */
    void apply(MachineModifier modifier);
    
    /**
     * Apply a state modification to this machine, 
     * even if the machine already is in the state
     * the modification will put it in.
     * @param modifier
     */
    void forceApply(MachineModifier modifier);

    /**
     * Verify something about this machine.
     * @param verifier
     */
    void verifyThat(Verifier verifier);
    
    /**
     * Get basic information about the underlying virtual
     * machine.
     * @return
     */
    VMDefinition getVMDefinition();

}
