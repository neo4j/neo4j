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

import static org.neo4j.vagrant.VMFactory.vm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.qa.driver.EnterpriseDriver;
import org.neo4j.qa.driver.WindowsEnterpriseDriver;
import org.neo4j.vagrant.VirtualMachine;

import scala.actors.threadpool.Arrays;

@RunWith(value = Parameterized.class)
public class EnterpriseQualityAssuranceTest {
    
    @Parameters
    @SuppressWarnings("unchecked")
    public static Collection<Object[]> testParameters()
    {
        
        VirtualMachine win1 = vm(Neo4jVM.WIN_1);
        VirtualMachine win2 = vm(Neo4jVM.WIN_2);
        VirtualMachine win3 = vm(Neo4jVM.WIN_3);
        
        Object[][] ps = new Object[][] { 
            { new EnterpriseDriver []{
              new WindowsEnterpriseDriver( win1, SharedConstants.WINDOWS_ENTERPRISE_INSTALLER ),
              new WindowsEnterpriseDriver( win2, SharedConstants.WINDOWS_ENTERPRISE_INSTALLER ),
              new WindowsEnterpriseDriver( win3, SharedConstants.WINDOWS_ENTERPRISE_INSTALLER ) 
            }}
        };
        
        return Arrays.asList(ps);
    }

    private EnterpriseDriver[] drivers;
    private int zooClientPortBase = 2181;

    public EnterpriseQualityAssuranceTest(EnterpriseDriver [] drivers)
    {
        this.drivers = drivers;
    }
    
    @After
    public void resetVMs() {
        for(EnterpriseDriver d : drivers) {
            d.close();
            //d.getVM().rollback();
        }
    }
    
    @Test
    public void highAvailabilityQualityAssuranceTest() {
        EnterpriseDriver d;
        List<String> coordinators = new ArrayList<String>();

        for( int i=0; i<drivers.length; i++) {
            d = drivers[i];
            d.up();
            d.runInstall();
            d.stopService();
            setupZookeeper(d, i + 1);
            coordinators.add(d.getVM().definition().ip() + ":" + (zooClientPortBase + i + 1));
        }
        
        String coordinatorsConfigValue = StringUtils.join(coordinators,",");
        
        for( int i=0; i<drivers.length; i++) {
            d = drivers[i];
            String neo4jConf = d.installDir() + "/conf/neo4j.properties";
            String serverConf = d.installDir() + "/conf/neo4j-server.properties";
            
            d.setConfig(neo4jConf, "ha.server_id", "" + (i+1));
            d.setConfig(neo4jConf, "ha.server", "0.0.0.0:6001");
            d.setConfig(neo4jConf, "ha.coordinators", coordinatorsConfigValue);
            
            d.setConfig(serverConf, "org.neo4j.server.database.mode", "HA");
            d.startService();
        }
        
        // At this point, we should have a running neo4j cluster.
    }
    
    private void setupZookeeper(EnterpriseDriver d, int serverId) {
        String zookeeperConf = d.installDir() + "/conf/coord.cfg";
        
        d.runZookeeperInstall();
        d.stopZookeeperService();
        for( int o=0; o<drivers.length; o++) {
            d.setConfig(zookeeperConf, "server." + (o+1), drivers[o].getVM().definition().ip() + ":2888:3888");
        }
        d.setConfig(zookeeperConf, "clientPort", (zooClientPortBase + serverId) + "");
        d.writeFile("" + serverId, d.installDir() + "/data/coordinator/myid");
        d.startZookeeperService();
    }
}
