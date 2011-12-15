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
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.vagrant.Box;
import org.neo4j.vagrant.CygwinShell;
import org.neo4j.vagrant.ShellException;
import org.neo4j.vagrant.Vagrant;

import com.sun.jersey.api.client.ClientHandlerException;

public class TheTest {

    protected String vagrantBaseDir()
    {
        return System.getProperty("user.dir") + "/target/test-classes/vagrant/";
    }

    protected Vagrant vagrant(Box baseBox)
    {
        return vagrant(baseBox, baseBox.getName() + "-vanilla");
    }

    protected Vagrant vagrant(Box box, String projectFolderName)
    {
        Vagrant v;
        File projectFolder = new File(vagrantBaseDir() + projectFolderName);
        if (!projectFolder.exists())
        {
            projectFolder.mkdirs();
            v = vagrant(box, projectFolder);
            v.init(box);
        } else
        {
            v = vagrant(box, projectFolder);
        }
        return v;
    }

    protected Vagrant vagrant(Box box, File projectFolder)
    {
        Vagrant v = new Vagrant(projectFolder);
        v.ensureBoxExists(box);
        return v;
    }

    @Test
    public void testInstallAndUninstall() throws Throwable
    {
        Vagrant v = vagrant(Box.WINDOWS_2008_R2_AMD64,
                "windows-2008R2-amd64-jre6");
        v.up();
        
        // Transaction tx = v.beginTx();
        try
        {

            v.copyFromHost(
                    System.getProperty("user.dir")
                            + "/target/classes/installer-windows-1.6-SNAPSHOT-windows-community.msi",
                    "/home/vagrant/installer.msi");

            CygwinShell sh = new CygwinShell(v.ssh());
            sh.run(":> install.log");
            sh.runDOS("msiexec /quiet /L*v install.log /i installer.msi INSTALL_DIR=\"C:\\det är dåligt. mycket mycket dåligt. gör inte såhär.\"");
            //checkDataRest();
            //sh.run(":> uninstall.log");
            //sh.runDOS("msiexec /quiet /L*v uninstall.log /x installer.msi");
            sh.close();

            try
            {
                checkDataRest();
                fail("Server is still listening to port 7474 even after uninstall");
            } catch (ClientHandlerException e)
            {
                // no-op
            }

        } catch (ShellException e)
        {
            System.out.println(e.getResult().getOutput());
            throw e;
        } finally
        {
            // tx.finish();
        }
    }

    private void checkDataRest() throws Exception
    {
        JaxRsResponse r = RestRequest.req().get(
                "http://localhost:27474/db/data/");
        assertThat(r.getStatus(), equalTo(200));
    }
}
