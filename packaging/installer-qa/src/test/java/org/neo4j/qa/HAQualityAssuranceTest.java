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
package org.neo4j.qa;

import static org.neo4j.qa.clusterstate.modifier.Neo4jClusterInstallation.neo4jClusterInstallation;
import static org.neo4j.qa.clusterstate.modifier.Neo4jHABackup.neo4jHABackup;
import static org.neo4j.qa.clusterstate.modifier.ZookeeperClusterInstallation.zookeeperClusterInstallation;
import static org.neo4j.qa.clusterstate.verifier.Neo4jHAClusterWorks.neo4jHAClusterWorks;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.qa.clusterstate.MachineClusterModel;
import org.neo4j.qa.runner.ClusterQualityAssurance;

@RunWith(ClusterQualityAssurance.class)
public class HAQualityAssuranceTest {
    
    protected final MachineClusterModel model;

    public HAQualityAssuranceTest(MachineClusterModel model)
    {
        this.model = model;
    }

    @Test
    public void enterpriseQualityAssurance()
    {
        model.apply(zookeeperClusterInstallation());
        model.apply(neo4jClusterInstallation());
        
        model.verifyThat(neo4jHAClusterWorks());
        
        model.apply(neo4jHABackup());
        
        model.verifyThat(neo4jHAClusterWorks());
    }
}
