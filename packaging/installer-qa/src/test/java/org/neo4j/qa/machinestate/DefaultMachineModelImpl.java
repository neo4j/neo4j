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
package org.neo4j.qa.machinestate;

import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.machinestate.modifier.MachineModifier;
import org.neo4j.qa.machinestate.modifier.RawStateModifier;
import org.neo4j.qa.machinestate.verifier.Verifier;
import org.neo4j.vagrant.VMDefinition;

public class DefaultMachineModelImpl implements MachineModel {

    private Neo4jDriver driver;

    private StateRegistry state = new StateRegistry();

    public DefaultMachineModelImpl(Neo4jDriver d)
    {
        this.driver = d;
    }

    @Override
    public void apply(MachineModifier modifier)
    {
    	System.out.println("[Machine "+getVMDefinition().ip()+"] Applying: '" + modifier + "' (" + modifier.getClass().getSimpleName() + ")");
        if (stateModelDiffersFrom(modifier.stateModifications()))
        {
            modifier.modify(driver, state);
        }

        updateStateModel(modifier);
    }

    @Override
    public void forceApply(MachineModifier modifier)
    {
    	System.out.println("[Machine "+getVMDefinition().ip()+"] Force applying: '" + modifier + "' (" + modifier.getClass().getSimpleName() + ")");
        modifier.modify(driver, state);
        updateStateModel(modifier);
    }

    @Override
    public void verifyThat(Verifier verifier)
    {
    	System.out.println("[Machine "+getVMDefinition().ip()+"] Verifying: '" + verifier + "' (" + verifier.getClass().getSimpleName() + ")");
        verifier.verify(driver);
    }

    @Override
    public VMDefinition getVMDefinition()
    {
        return driver.vm().definition();
    }

    private void updateStateModel(MachineModifier modifier)
    {
        if (modifier instanceof RawStateModifier)
        {
            ((RawStateModifier) modifier).modifyRawState(state);
        } else
        {
            for (StateAtom atom : modifier.stateModifications())
            {
                state.put(atom);
            }
        }
    }

    private boolean stateModelDiffersFrom(StateAtom[] stateModifications)
    {
        if (stateModifications != null)
        {
            for (StateAtom atom : stateModifications)
            {
                if (!state.contains(atom)
                        || state.get(atom).value() != atom.value())
                {
                    return true;
                }
            }
        }
        return false;
    }

}
