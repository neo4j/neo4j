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
package org.neo4j.qa.clusterstate;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.neo4j.qa.machinestate.StateAtom;
import org.neo4j.qa.machinestate.ZookeeperNodeDescription;

public class ZookeeperClusterDescription implements StateAtom {

    private List<ZookeeperNodeDescription> nodes = new ArrayList<ZookeeperNodeDescription>();
    
    public ZookeeperClusterDescription() {
        
    }
    
    public List<ZookeeperNodeDescription> getNodes()
    {
        return nodes;
    }

    @Override
    public Object value()
    {
        return true;
    }

    public void addNode(ZookeeperNodeDescription zookeeperNodeDescription)
    {
        nodes.add(zookeeperNodeDescription);
    }

    public String getAdressString()
    {
        List<String> coordinators = new ArrayList<String>();
        for(ZookeeperNodeDescription node : getNodes()) {
            coordinators.add(node.getClientConnectionURL());
        }
        return StringUtils.join(coordinators, ",");
    }

}
