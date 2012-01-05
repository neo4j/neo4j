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
import org.junit.Before;
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
            { "windows",
              new EnterpriseDriver []{
              new WindowsEnterpriseDriver( win1, 
                      SharedConstants.WINDOWS_ENTERPRISE_INSTALLER, 
                      SharedConstants.WINDOWS_COORDINATOR_INSTALLER),
              new WindowsEnterpriseDriver( win2, 
                      SharedConstants.WINDOWS_ENTERPRISE_INSTALLER, 
                      SharedConstants.WINDOWS_COORDINATOR_INSTALLER ),
              new WindowsEnterpriseDriver( win3, 
                      SharedConstants.WINDOWS_ENTERPRISE_INSTALLER, 
                      SharedConstants.WINDOWS_COORDINATOR_INSTALLER ) 
            }}
        };
        
        return Arrays.asList(ps);
    }

    private EnterpriseDriver[] drivers;
    private int zooClientPortBase = 2181;
    private String coordinatorsConfigValue;
    private String testName;

    public EnterpriseQualityAssuranceTest(String testName, EnterpriseDriver [] drivers)
    {
        this.testName = testName;
        this.drivers = drivers;
    }
    
    @Before
    public void resetVMs() {
        for(EnterpriseDriver d : drivers) {
            //d.close();
            d.vm().up();
            d.vm().rollback();
        }
    }
    
    @After
    public void asd() {
//        String logDir = SharedConstants.TEST_LOGS_DIR + testName + "/";
//        new File(logDir).mkdirs();
//        for(EnterpriseDriver d : drivers) {
//            String logBase = logDir + d.vm().definition().ip() + ".";
//            try {
//                d.vm().copyFromVM(d.installDir() + "/data/graph.db/messages.log", logBase + "messages.log");
//                d.vm().copyFromVM(d.installDir() + "/data/log/neo4j.0.0.log", logBase + "neo4j.0.0.log");
//                d.vm().copyFromVM(d.installDir() + "/data/log/neo4j-zookeeper.log", logBase + "zookeeper-client.log");
//                d.vm().copyFromVM(d.zookeeperInstallDir() + "/data/log/neo4j-zookeeper.log", logBase + "zookeeper-server.log");
//            } catch(Exception e) {
//                e.printStackTrace();
//            }
//        }
    }
    
    @Test
    public void highAvailabilityQualityAssuranceTest() {
        
        // Start servers
        for(EnterpriseDriver d : drivers ) d.up();
        
        setupZookeeperCluster();
        
        setupHighAvailabilityCluster();
        
        assertClusterWorks();
        
        // Restart a server
        drivers[0].reboot();
        //drivers[0].vm().halt();
        
        assertClusterWorks();
        
        assertHABackupWorks();
        
    }

    private void assertHABackupWorks()
    {
        EnterpriseDriver driver = drivers[0];
        
        //driver.performBackup();
    }

    /*
     * Rotate over available cluster machines a number of times.
     * Take turns creating, fetching, and deleting nodes via 
     * different machines.
     */
    private void assertClusterWorks()
    {
        int rounds = 4;
        long nodeId = -1;
        for(int i=0; i < drivers.length * rounds; i++) {
            EnterpriseDriver driver = drivers[i % drivers.length];
            try {
                switch(i % 4) {
                case 0:
                    nodeId = driver.api().createNode();
                    break;
                case 1:
                    driver.api().waitUntilNodeExists(nodeId);
                    break;
                case 2: 
                    driver.api().waitUntilNodeExists(nodeId);
                    driver.api().deleteNode(nodeId);
                    break;
                case 3:
                    driver.api().waitUntilNodeDoesNotExist(nodeId);
                    break;
                }
            } catch(Exception e) {
                throw new RuntimeException("Ensuring HA cluster works failed while executing request on server " + driver.vm().definition().vmName() + ". See nested exception.", e);
            }
        }
    }

    private void setupZookeeperCluster()
    {
        List<String> coordinators = new ArrayList<String>();
        EnterpriseDriver driver;
        
        for( int i=0; i<drivers.length; i++) 
        {
            driver = drivers[i];
            setupZookeeper(driver, i + 1);
            coordinators.add(driver.vm().definition().ip() + ":" + (zooClientPortBase + i + 1));
        }
        
        coordinatorsConfigValue = StringUtils.join(coordinators,",");
    }
    
    private void setupHighAvailabilityCluster()
    {
        EnterpriseDriver driver;
        
        for( int i=0; i<drivers.length; i++) 
        {
            driver = drivers[i];
            
            String neo4jConf = driver.installDir() + "/conf/neo4j.properties";
            String serverConf = driver.installDir() + "/conf/neo4j-server.properties";
            
            driver.runInstall();
            driver.stopService();
            
            driver.setConfig(neo4jConf, "ha.cluster_name", "mycluster");
            driver.setConfig(neo4jConf, "ha.server_id", "" + (i+1));
            driver.setConfig(neo4jConf, "ha.server", driver.vm().definition().ip() + ":6001");
            driver.setConfig(neo4jConf, "ha.coordinators", coordinatorsConfigValue);
            
            driver.setConfig(serverConf, "org.neo4j.server.database.mode", "HA");
            driver.setConfig(serverConf, "org.neo4j.server.webserver.address", "0.0.0.0");
            
            // The database folder has to be empty on first boot
            driver.destroyDatabase();
            
            driver.startService();
        }
    }
    
    private void setupZookeeper(EnterpriseDriver d, int serverId) {
        String zookeeperConf = d.zookeeperInstallDir() + "/conf/coord.cfg";
        
        d.runZookeeperInstall();
        d.stopZookeeperService();
        for( int o=0; o<drivers.length; o++) {
            d.setConfig(zookeeperConf, "server." + (o+1), drivers[o].vm().definition().ip() + ":2888:3888");
        }
        d.setConfig(zookeeperConf, "clientPort", (zooClientPortBase + serverId) + "");
        d.writeFile("" + serverId, d.zookeeperInstallDir() + "/data/coordinator/myid");
        d.startZookeeperService();
    }
}
