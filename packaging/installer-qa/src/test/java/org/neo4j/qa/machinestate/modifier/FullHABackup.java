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
package org.neo4j.qa.machinestate.modifier;

import org.neo4j.qa.clusterstate.ZookeeperClusterDescription;
import org.neo4j.qa.driver.EnterpriseDriver;
import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.machinestate.StateAtom;
import org.neo4j.qa.machinestate.StateRegistry;

public class FullHABackup implements MachineModifier {

    private String name;
    private ZookeeperClusterDescription zkCluster;

    public FullHABackup(String name, ZookeeperClusterDescription zkCluster) {
        this.name = name;
        this.zkCluster = zkCluster;
    }
    
    @Override
    public void modify(Neo4jDriver driver, StateRegistry state)
    {
        if(driver instanceof EnterpriseDriver) {
            ((EnterpriseDriver)driver).performFullHABackup(name, zkCluster.getAdressString());
        }
    }

    @Override
    public StateAtom[] stateModifications()
    {
        return new StateAtom[]{};
    }

}
