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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.vagrant.VMFactory.vm;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.vagrant.Box;
import org.neo4j.vagrant.CygwinShell;
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.VirtualMachine;

import com.sun.jersey.api.client.ClientHandlerException;

public class BasicQAWindowsTest {

    private String installerDir = System.getProperty("neo4j.installer-dir", System.getProperty("user.dir") + "/target/classes/");
    
    private static final String INSTALL_PATH = "/cygdrive/c/neo4j\\ install\\ with\\ spaces";
    private static final String NEO4J_VERSION = System.getProperty("neo4j.version", "NO-NEO4J.VERSION-ENV-VAR-SET");
    
    private static VirtualMachine vm;
    private static CygwinShell sh;

    @BeforeClass
    public static void provisionVM() {
        vm = vm(Box.WINDOWS_2008_R2_AMD64, "win2008", "windows-2008R2-amd64-plain");
        vm.up();
    }
    
    @Before
    public void connect() {
        sh = new CygwinShell(vm.ssh());
    }
    
    @After
    public void rollbackChanges() {
        sh.close();
        vm.rollback();
    }

    @Test
    public void testCommunityInstallAndUninstall() throws Throwable
    {
        String path = installerDir + "installer-windows-community.msi";
        basicQAChecklist(path);
    }

    @Test
    public void testAdvancedInstallAndUninstall() throws Throwable
    {
        String path = installerDir + "installer-windows-advanced.msi";
        basicQAChecklist(path);
    }

    @Test
    public void testEnterpriseInstallAndUninstall() throws Throwable
    {
        String path = installerDir + "installer-windows-enterprise.msi";
        basicQAChecklist(path);
    }
    
    private void basicQAChecklist(String installerPath) throws Exception {
        vm.copyFromHost(installerPath , "/home/vagrant/installer.msi");

        sh.run(":> install.log");
        sh.runDOS("msiexec /quiet /L* install.log /i installer.msi INSTALL_DIR=\"C:\\neo4j install with spaces\"");
        
        // We need to make the server allow requests from the outside
        sh.run("net stop neo4j");
        sh.run("echo org.neo4j.server.webserver.address=0.0.0.0 >> "+INSTALL_PATH+"/conf/neo4j-server.properties");
        sh.run("net start neo4j");
        
        assertDocsAreCorrect();
        assertRESTWorks();
        
        sh.run("net stop neo4j");
        
        sh.run(":> uninstall.log");
        sh.runDOS("msiexec /quiet /L*v uninstall.log /x installer.msi");

        try
        {
            assertRESTWorks();
            fail("Server is still listening to port 7474 even after uninstall");
        } catch (ClientHandlerException e)
        {
            // no-op
        }
    }

    private void assertDocsAreCorrect()
    {
        Result r = sh.run("cat " + INSTALL_PATH + "/doc/manual/html/index.html | grep " + NEO4J_VERSION);
        assertThat(r.getOutput(), containsString(NEO4J_VERSION));
        
        r = sh.run("cat " + INSTALL_PATH + "/doc/manual/text/neo4j-manual.txt | grep " + NEO4J_VERSION);
        assertThat(r.getOutput(), containsString(NEO4J_VERSION));
        
        r = sh.run("ls " + INSTALL_PATH + "/doc/manual/pdf");
        assertThat(r.getOutput(), containsString("neo4j-manual.pdf"));
    }

    private void assertRESTWorks() throws Exception
    {
        JaxRsResponse r = RestRequest.req().get(
                "http://localhost:7474/db/data/");
        assertThat(r.getStatus(), equalTo(200));
    }
}
