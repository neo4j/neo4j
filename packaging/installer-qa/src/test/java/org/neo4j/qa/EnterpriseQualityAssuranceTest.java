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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.qa.driver.EnterpriseDriver;
import org.neo4j.qa.driver.UbuntuDebEnterpriseDriver;
import org.neo4j.qa.driver.WindowsEnterpriseDriver;
import org.neo4j.vagrant.VirtualMachine;

@RunWith(value = Parameterized.class)
public class EnterpriseQualityAssuranceTest {
    
    @Parameters
    public static Collection<Object[]> testParameters()
    {

        Map<String, Object[]> platforms = new HashMap<String, Object[]>();
        List<Object[]> testParameters = new ArrayList<Object[]>();
        
        VirtualMachine win1 = vm(Neo4jVM.WIN_1);
        VirtualMachine win2 = vm(Neo4jVM.WIN_2);
        VirtualMachine win3 = vm(Neo4jVM.WIN_3);
        
        VirtualMachine ubuntu1 = vm(Neo4jVM.UBUNTU_1);
        VirtualMachine ubuntu2 = vm(Neo4jVM.UBUNTU_2);
        VirtualMachine ubuntu3 = vm(Neo4jVM.UBUNTU_3);
        
        // Windows
        platforms.put(Platforms.WINDOWS, new Object[] { 
                EnterpriseQualityAssuranceTest.class.getName() + "_" + WindowsEnterpriseDriver.class.getName(),
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
                }});
        
        // Ubuntu, with debian installer
        platforms.put(Platforms.UBUNTU_DEB, new Object[] { 
                EnterpriseQualityAssuranceTest.class.getName() + "_" + UbuntuDebEnterpriseDriver.class.getName(),
                new EnterpriseDriver []{
                    new UbuntuDebEnterpriseDriver( ubuntu1, 
                            SharedConstants.DEBIAN_ENTERPRISE_INSTALLER, 
                            SharedConstants.DEBIAN_COORDINATOR_INSTALLER  ),
                    new UbuntuDebEnterpriseDriver( ubuntu2, 
                            SharedConstants.DEBIAN_ENTERPRISE_INSTALLER, 
                            SharedConstants.DEBIAN_COORDINATOR_INSTALLER  ),
                    new UbuntuDebEnterpriseDriver( ubuntu3, 
                            SharedConstants.DEBIAN_ENTERPRISE_INSTALLER, 
                            SharedConstants.DEBIAN_COORDINATOR_INSTALLER  ) 
                }});
        
        for(String platform : Platforms.getPlaformsToUse()) {
            testParameters.add(platforms.get(platform));
        }
        
        return testParameters;
    }

    private EnterpriseDriver[] drivers;
    private int zooClientPortBase = 2181;
    private String coordinatorAddresses;
    private String testName;

    public EnterpriseQualityAssuranceTest(String testName, EnterpriseDriver [] drivers)
    {
        this.testName = testName;
        this.drivers = drivers;
    }
    
    @Before
    public void resetVMs() {
        for(EnterpriseDriver d : drivers) {
            d.up();
            d.vm().rollback();
        }
    }
    
    @After
    public void downloadLogs() {
        String logDir = SharedConstants.TEST_LOGS_DIR + testName;
        new File(logDir).mkdirs();
        for(EnterpriseDriver d : drivers) {
            try {
                d.downloadLogsTo(logDir);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    @Test
    public void highAvailabilityQualityAssuranceTest() {
        
        // Start servers
        for(EnterpriseDriver d : drivers ) d.up();
        
        setupZookeeperCluster();
        
        setupHighAvailabilityCluster();
        
        assertClusterWorks();
        
        // TODO: This currently fails, the cluster does not
        // work when it comes back up.
        // Restart a server
        //drivers[0].reboot();
        //assertClusterWorks();
        
        assertHABackupWorks();
        
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
        
        coordinatorAddresses = StringUtils.join(coordinators,",");
    }
    
    private void setupZookeeper(EnterpriseDriver d, int serverId) {
        String zookeeperConf = d.zookeeperInstallDir() + "/conf/coord.cfg";
        
        d.installZookeeper();
        d.stopZookeeper();
        for( int o=0; o<drivers.length; o++) {
            d.setConfig(zookeeperConf, "server." + (o+1), drivers[o].vm().definition().ip() + ":2888:3888");
        }
        d.setConfig(zookeeperConf, "clientPort", (zooClientPortBase + serverId) + "");
        d.writeFile("" + serverId, d.zookeeperInstallDir() + "/data/coordinator/myid");
        d.startZookeeper();
    }

    private void setupHighAvailabilityCluster()
    {
        EnterpriseDriver driver;
        
        for( int i=0; i<drivers.length; i++) 
        {
            driver = drivers[i];
            
            String neo4jConf = driver.neo4jInstallDir() + "/conf/neo4j.properties";
            String serverConf = driver.neo4jInstallDir() + "/conf/neo4j-server.properties";
            
            driver.installNeo4j();
            driver.stopNeo4j();
            
            driver.setConfig(neo4jConf, "ha.server_id", "" + (i+1));
            driver.setConfig(neo4jConf, "ha.server", driver.vm().definition().ip() + ":6001");
            driver.setConfig(neo4jConf, "ha.coordinators", coordinatorAddresses);
            
            driver.setConfig(serverConf, "org.neo4j.server.database.mode", "HA");
            driver.setConfig(serverConf, "org.neo4j.server.webserver.address", "0.0.0.0");
            
            // The database folder has to be empty on first boot
            driver.deleteDatabase();
            
            driver.startNeo4j();
        }
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
                    nodeId = driver.neo4jClient().createNode();
                    break;
                case 1:
                    driver.neo4jClient().waitUntilNodeExists(nodeId);
                    break;
                case 2: 
                    driver.neo4jClient().waitUntilNodeExists(nodeId);
                    driver.neo4jClient().deleteNode(nodeId);
                    break;
                case 3:
                    driver.neo4jClient().waitUntilNodeDoesNotExist(nodeId);
                    break;
                }
            } catch(Exception e) {
                throw new RuntimeException("Ensuring HA cluster works failed while executing request on server " + driver.vm().definition().vmName() + ". See nested exception.", e);
            }
        }
    }

    private void assertHABackupWorks()
    {
        EnterpriseDriver driver = drivers[0];
        
        long nodeId = driver.neo4jClient().createNode();
        
        driver.performFullHABackup("neobackup", coordinatorAddresses);
        driver.performIncrementalHABackup("neobackup", coordinatorAddresses);
        
        // Shut down the cluster
        for(EnterpriseDriver d : drivers) {
            d.stopNeo4j();
            d.deleteDatabase();
        }
        
        driver.replaceGraphDataDirWithBackup("neobackup");
        
        // Start the cluster back up
        for(EnterpriseDriver d : drivers) d.startNeo4j();
        
        // Wait for all databases to be up to date
        for(EnterpriseDriver d : drivers) {
            try {
                d.neo4jClient().waitUntilNodeExists(nodeId);
            } catch(Exception e){
                throw new RuntimeException("Restoring backup failed on server " + d.vm().definition().ip(), e);
            }
        }
        
        assertClusterWorks();
    }
}
