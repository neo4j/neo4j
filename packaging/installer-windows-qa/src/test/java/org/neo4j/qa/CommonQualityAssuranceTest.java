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

import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.driver.WindowsAdvancedDriver;
import org.neo4j.qa.driver.WindowsCommunityDriver;
import org.neo4j.qa.driver.WindowsEnterpriseDriver;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.vagrant.VirtualMachine;

import scala.actors.threadpool.Arrays;

import com.sun.jersey.api.client.ClientHandlerException;

@RunWith(value = Parameterized.class)
public class CommonQualityAssuranceTest {

    private VirtualMachine vm;
    private Neo4jDriver driver;

    @Parameters
    @SuppressWarnings("unchecked")
    public static Collection<Object[]> testParameters()
    {
        
        VirtualMachine windows = vm(Neo4jVM.WIN_1);
        
        Object[][] ps = new Object[][] { 
                { new WindowsCommunityDriver(  windows, SharedConstants.WINDOWS_COMMUNITY_INSTALLER )  },
                { new WindowsAdvancedDriver(   windows, SharedConstants.WINDOWS_ADVANCED_INSTALLER )   },
                { new WindowsEnterpriseDriver( windows, 
                        SharedConstants.WINDOWS_ENTERPRISE_INSTALLER, 
                        SharedConstants.WINDOWS_COORDINATOR_INSTALLER ) } };
        
        return Arrays.asList(ps);
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
    }

    @After
    public void rollbackChanges()
    {
        driver.close();
        vm.rollback(); // We'll be reusing this vm.
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
                "http://localhost:7474/db/data/");
        assertThat(r.getStatus(), equalTo(200));
    }

}
