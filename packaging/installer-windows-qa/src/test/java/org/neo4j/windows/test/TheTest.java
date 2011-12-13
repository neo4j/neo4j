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
package org.neo4j.windows.test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Test;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.vagrant.Box;
import org.neo4j.vagrant.SSHShell;
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.ShellException;
import org.neo4j.vagrant.Vagrant;

public class TheTest {
    
    protected String vagrantBaseDir() {
        return System.getProperty("user.dir") + "/target/test-classes/vagrant/";
    }
    
    protected Vagrant vagrant(Box baseBox) {
        return vagrant(baseBox, baseBox.getName() + "-vanilla");
    }
    
    protected Vagrant vagrant(Box box, String projectFolderName) {
        Vagrant v;
        File projectFolder = new File(vagrantBaseDir() + projectFolderName);
        if(!projectFolder.exists()) {
            projectFolder.mkdirs();
            v = vagrant(box, projectFolder);
            v.init(box);
        } else {
            v = vagrant(box, projectFolder);
        }
        return v;
    }
    
    protected Vagrant vagrant(Box box, File projectFolder) {
        Vagrant v = new Vagrant(projectFolder);
        v.ensureBoxExists(box);
        return v;
    }
    
    @Test 
    public void testInstallAndUninstall() throws Throwable {
        
        Vagrant v = vagrant(Box.WINDOWS_2008_R2_AMD64, "windows-2008R2-amd64-plain");
        
        try {
            v.up();
            
            v.copyFromHost(System.getProperty("user.dir") + "/target/test-classes/test", "/home/vagrant");

            SSHShell vm = v.ssh();
            Result r = vm.run("cat /home/vagrant/test");
            System.out.println(r.getOutput());
        } catch(ShellException e) {
            System.out.println(e.getResult().getOutput());
        } finally {
//            v.destroy();
        }
//        Result install = run("msiexec /i target\\neo4j-community-setup-1.6-SNAPSHOT.msi /quiet INSTALL_DIR=\"C:\\det är dåligt. mycket mycket dåligt. gör inte såhär.\"");
//        install.checkResults();
//        checkDataRest();
//
//        Result uninstall = run("msiexec /x target\\neo4j-community-setup-1.6-SNAPSHOT.msi /quiet");
//        uninstall.checkResults();
//        try {
//            checkDataRest();
//            fail("Server is still listening to port 7474 even after uninstall");
//        } catch (ClientHandlerException e) {
//            // no-op
//        }
    }
    
    

    private void checkDataRest() throws Exception {
        JaxRsResponse r = RestRequest.req().get(
                "http://localhost:7474/db/data/");
        assertThat(r.getStatus(), equalTo(200));
    }
}
