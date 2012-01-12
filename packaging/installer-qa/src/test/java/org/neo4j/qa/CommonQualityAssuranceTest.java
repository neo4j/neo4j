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
import org.neo4j.qa.driver.UbuntuDebCommunityDriver;
import org.neo4j.qa.driver.UbuntuDebEnterpriseDriver;
import org.neo4j.qa.driver.WindowsAdvancedDriver;
import org.neo4j.qa.driver.WindowsCommunityDriver;
import org.neo4j.qa.driver.WindowsEnterpriseDriver;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.vagrant.VirtualMachine;

import com.sun.jersey.api.client.ClientHandlerException;

@RunWith(value = Parameterized.class)
public class CommonQualityAssuranceTest {

    private VirtualMachine vm;
    private Neo4jDriver driver;

    @Parameters
    public static Collection<Object[]> testParameters()
    {
        Map<String, Neo4jDriver[]> platforms = new HashMap<String, Neo4jDriver[]>();
        List<Object[]> testParameters = new ArrayList<Object[]>();
        
        VirtualMachine windows = vm(Neo4jVM.WIN_1);
        VirtualMachine ubuntu = vm(Neo4jVM.UBUNTU_1);
        
        // Windows
        platforms.put(Platforms.WINDOWS, new Neo4jDriver[] {
            new WindowsCommunityDriver(  windows, SharedConstants.WINDOWS_COMMUNITY_INSTALLER ),
            new WindowsAdvancedDriver(   windows, SharedConstants.WINDOWS_ADVANCED_INSTALLER ),
            new WindowsEnterpriseDriver( windows, 
                    SharedConstants.WINDOWS_ENTERPRISE_INSTALLER, 
                    SharedConstants.WINDOWS_COORDINATOR_INSTALLER ) });
        
        // Ubuntu, with debian installer
        platforms.put(Platforms.UBUNTU_DEB, new Neo4jDriver[] {
                new UbuntuDebCommunityDriver(  ubuntu, SharedConstants.UBUNTU_COMMUNITY_INSTALLER ),
                new UbuntuDebAdvancedDriver(  ubuntu, SharedConstants.UBUNTU_ADVANCED_INSTALLER ),
                new UbuntuDebEnterpriseDriver(  ubuntu, SharedConstants.UBUNTU_ENTERPRISE_INSTALLER, 
                                                        SharedConstants.UBUNTU_COORDINATOR_INSTALLER )});
        
        for(String platform : Platforms.getPlaformsToUse()) {
            for(Neo4jDriver d : platforms.get(platform)) {
                testParameters.add(new Object[]{d});
            }
        }
        
        return testParameters;
    }

    public CommonQualityAssuranceTest(Neo4jDriver driver)
    {
        this.driver = driver;
        this.vm = driver.vm();
    }

    @Before
    public void boot()
    {
        driver.up();
        vm.rollback();
    }

    @After
    public void rollbackChanges()
    {
        driver.close();
        // We don't roll back after a test, it's done before.
        // This is bad practice (we leave the VM dirty), but
        // it makes debugging significantly easier because
        // the VM can be inspected.
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
        driver.runInstall();
        driver.stopService();
        driver.setConfig(driver.installDir() + "/conf/neo4j-server.properties","org.neo4j.server.webserver.address", "0.0.0.0");
        driver.startService();
        assertRESTWorks();
    }

    private void assertUninstallWorks() throws Throwable
    {
        driver.runUninstall();
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
        driver.stopService();
        assertRESTDoesNotWork();
        driver.startService();
        assertRESTWorks();
    }

    private void assertDocumentationIsCorrect() throws Throwable
    {
        String file = driver.readFile(driver.installDir() + "/doc/manual/html/index.html");
        assertThat(file, containsString(SharedConstants.NEO4J_VERSION));

        List<String> files = driver.listDir(driver.installDir() + "/doc/manual/text");
        assertThat(files, hasItem("neo4j-manual.txt"));

        files = driver.listDir(driver.installDir() + "/doc/manual/pdf");
        assertThat(files, hasItem("neo4j-manual.pdf"));
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
