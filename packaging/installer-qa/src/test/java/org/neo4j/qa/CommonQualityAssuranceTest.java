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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.vagrant.VMFactory.vm;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.driver.UbuntuDebAdvancedDriver;
import org.neo4j.qa.driver.UbuntuDebBaseDriver;
import org.neo4j.qa.driver.UbuntuDebEnterpriseDriver;
import org.neo4j.qa.driver.UbuntuTarGzBaseDriver;
import org.neo4j.qa.driver.UbuntuTarGzEnterpriseDriver;
import org.neo4j.qa.driver.WindowsBaseDriver;
import org.neo4j.qa.driver.WindowsEnterpriseDriver;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.vagrant.VirtualMachine;

import com.sun.jersey.api.client.ClientHandlerException;


/**
 * TODO: Using JUnit to run theses may not have been a great idea,
 * consider refactoring this into a "real" java program rather
 * than a unit test. Same goes for {@link EnterpriseQualityAssuranceTest}
 *
 */
@RunWith(value = Parameterized.class)
public class CommonQualityAssuranceTest {

    private VirtualMachine vm;
    private Neo4jDriver driver;
    private String testName;

    @Parameters
    public static Collection<Object[]> testParameters()
    {
        Map<String, Neo4jDriver[]> platforms = new HashMap<String, Neo4jDriver[]>();
        List<Object[]> testParameters = new ArrayList<Object[]>();
        
        VirtualMachine windows = vm(Neo4jVM.WIN_1);
        VirtualMachine ubuntu = vm(Neo4jVM.UBUNTU_1);
        
        // Windows
        platforms.put(Platforms.WINDOWS, new Neo4jDriver[] {
            new WindowsBaseDriver(   windows, SharedConstants.WINDOWS_COMMUNITY_INSTALLER ),
            new WindowsBaseDriver(   windows, SharedConstants.WINDOWS_ADVANCED_INSTALLER ),
            new WindowsEnterpriseDriver( windows, 
                    SharedConstants.WINDOWS_ENTERPRISE_INSTALLER, 
                    SharedConstants.WINDOWS_COORDINATOR_INSTALLER ) });
        
        // Ubuntu, with debian installer
        platforms.put(Platforms.UBUNTU_DEB, new Neo4jDriver[] {
                new UbuntuDebBaseDriver(        ubuntu, SharedConstants.DEBIAN_COMMUNITY_INSTALLER ),
                new UbuntuDebAdvancedDriver(    ubuntu, SharedConstants.DEBIAN_ADVANCED_INSTALLER ),
                new UbuntuDebEnterpriseDriver(  ubuntu, SharedConstants.DEBIAN_ENTERPRISE_INSTALLER, 
                                                        SharedConstants.DEBIAN_COORDINATOR_INSTALLER )});
        
        // Ubuntu, with tarball packages
        platforms.put(Platforms.UBUNTU_TAR_GZ, new Neo4jDriver[] {
                new UbuntuTarGzBaseDriver(        ubuntu, SharedConstants.UNIX_COMMUNITY_TARBALL ),
                new UbuntuTarGzBaseDriver(        ubuntu, SharedConstants.UNIX_ADVANCED_TARBALL ),
                new UbuntuTarGzEnterpriseDriver(  ubuntu, SharedConstants.UNIX_ENTERPRISE_TARBALL, 
                                                          SharedConstants.UNIX_ENTERPRISE_TARBALL )});
        
        for(String platform : Platforms.getPlaformsToUse()) {
            for(Neo4jDriver d : platforms.get(platform)) 
            {
                String name = CommonQualityAssuranceTest.class.getName() + "_" + d.getClass().getName();
                testParameters.add(new Object[]{name, d});
            }
        }
        
        return testParameters;
    }

    public CommonQualityAssuranceTest(String testName, Neo4jDriver driver)
    {
        this.driver = driver;
        this.testName = testName;
        this.vm = driver.vm();
    }

    @Before
    public void boot()
    {
        driver.up();
        vm.rollback();
    }

    @After
    public void cleanUp()
    {
        String logDir = SharedConstants.TEST_LOGS_DIR + testName;
        new File(logDir).mkdirs();
        try {
            driver.downloadLogsTo(logDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
        driver.close();
    }

    /*
     * Written as a single test, so that we don't have
     * to run the install more than once per edition (to save build time).
     */
    @Test
    public void basicQualityAssurance() throws Throwable
    {
        assertInstallWorks();
        assertDocumentationIsCorrect();
        assertServiceStartStopWorks();
        assertServerStartsAfterReboot();
        assertUninstallWorks();
    }
    
    private void assertInstallWorks() throws Throwable
    {
        driver.installNeo4j();
        driver.stopNeo4j();
        driver.setConfig(driver.neo4jInstallDir() + "/conf/neo4j-server.properties","org.neo4j.server.webserver.address", "0.0.0.0");
        driver.startNeo4j();
        assertRESTWorks();
    }

    private void assertUninstallWorks() throws Throwable
    {
        driver.uninstallNeo4j();
        assertRESTDoesNotWork();
        driver.reboot();
        assertRESTDoesNotWork();
    }

    private void assertServerStartsAfterReboot() throws Throwable
    {
        assertRESTWorks();
        driver.reboot();
        assertRESTWorks();
    }

    private void assertServiceStartStopWorks() throws Throwable
    {
        assertRESTWorks();
        driver.stopNeo4j();
        assertRESTDoesNotWork();
        driver.startNeo4j();
        assertRESTWorks();
    }

    private void assertDocumentationIsCorrect() throws Throwable
    {
        String file = driver.readFile(driver.neo4jInstallDir() + "/doc/manual/html/index.html");
        assertThat(file, containsString(SharedConstants.NEO4J_VERSION));

        List<String> files = driver.listDir(driver.neo4jInstallDir() + "/doc/manual/text");
        assertThat(files, hasItem("neo4j-manual.txt"));
    }

    private void assertRESTDoesNotWork() throws Exception
    {
        try
        {
            assertRESTWorks();
            fail("Server is still listening to port 7474, was expecting server to be turned off.");
        } catch (ClientHandlerException e)
        {
            // no-op
        }
    }

    private void assertRESTWorks() throws Exception
    {
        JaxRsResponse r = RestRequest.req().get(
                "http://"+vm.definition().ip()+":7474/db/data/");
        assertThat(r.getStatus(), equalTo(200));
    }

}
