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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.vagrant.VMFactory.vm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.vagrant.Box;
import org.neo4j.vagrant.CygwinShell;
import org.neo4j.vagrant.VirtualMachine;

import com.sun.jersey.api.client.ClientHandlerException;

public class WindowsInstallAndUninstallTest {

    private String installerDir = System.getProperty("neo4j.installer-dir", System.getProperty("user.dir") + "/target/classes/");
    
    private VirtualMachine vm;
    private CygwinShell sh;

    @Before
    public void provisionVM() {
        vm = vm(Box.WINDOWS_2008_R2_AMD64, "win2008", "windows-2008R2-amd64-plain");
        vm.up();

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
        testInstallAndUninstall(path);
    }

    @Test
    public void testAdvancedInstallAndUninstall() throws Throwable
    {
        String path = installerDir + "installer-windows-advanced.msi";
        testInstallAndUninstall(path);
    }

    @Test
    public void testEnterpriseInstallAndUninstall() throws Throwable
    {
        String path = installerDir + "installer-windows-enterprise.msi";
        testInstallAndUninstall(path);
    }
    
    private void testInstallAndUninstall(String installerPath) throws Exception {
        vm.copyFromHost(installerPath , "/home/vagrant/installer.msi");

        sh.run(":> install.log");
        sh.runDOS("msiexec /quiet /L* install.log /i installer.msi INSTALL_DIR=\"C:\\neo4j install with spaces\"");
        
        // We need to make the server allow requests from the outside
        sh.run("net stop neo4j");
        sh.run("echo org.neo4j.server.webserver.address=0.0.0.0 >> /cygdrive/c/neo4j\\ install\\ with\\ spaces/conf/neo4j-server.properties");
        sh.run("net start neo4j");
        
        checkDataRest();
        
        sh.run("net stop neo4j");
        
        sh.run(":> uninstall.log");
        sh.runDOS("msiexec /quiet /L*v uninstall.log /x installer.msi");

        try
        {
            checkDataRest();
            fail("Server is still listening to port 7474 even after uninstall");
        } catch (ClientHandlerException e)
        {
            // no-op
        }
    }

    private void checkDataRest() throws Exception
    {
        JaxRsResponse r = RestRequest.req().get(
                "http://localhost:7474/db/data/");
        assertThat(r.getStatus(), equalTo(200));
    }
}
