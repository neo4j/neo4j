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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
        return System.getProperty("jagrant.basedir", System.getProperty("user.home") + "/vm-tester/");
    }
    
    protected String templateBaseDir() {
        return System.getProperty("jagrant.templatedir", System.getProperty("user.dir") + "/target/test-classes/vagrant/");
    }

    protected Vagrant vagrant(Box baseBox, String name)
    {
        return vagrant(baseBox, name, baseBox.getName() + "-vanilla");
    }

    protected Vagrant vagrant(Box box, String name, String templateProject)
    {
        Vagrant v;
        File projectFolder = new File(vagrantBaseDir() + name);
        File templateFolder = new File(templateBaseDir() + templateProject);
        if (!projectFolder.exists())
        {
            projectFolder.mkdirs();
            v = vagrant(box, projectFolder);
            if(!templateFolder.exists()) {
                System.out.println("No vagrant project for " + templateProject + " ("+templateFolder.getAbsolutePath()+"), using vagrant init.");
                v.init(box);
            } else {
                copyFolder(templateFolder, projectFolder);
            }
        } else
        {
            v = vagrant(box, projectFolder);
        }
        return v;
    }

    protected Vagrant vagrant(Box box, File projectFolder)
    {
        Vagrant v = new Vagrant(projectFolder, true);
        v.ensureBoxExists(box);
        return v;
    }

    public static void copyFolder(File src, File dest)
    {
        try {
            if (src.isDirectory())
            {
    
                // if directory not exists, create it
                if (!dest.exists())
                {
                    dest.mkdir();
                }
    
                // list all the directory contents
                String files[] = src.list();
    
                for (String file : files)
                {
                    // construct the src and dest file structure
                    File srcFile = new File(src, file);
                    File destFile = new File(dest, file);
                    // recursive copy
                    copyFolder(srcFile, destFile);
                }
    
            } else
            {
                // if file, then copy it
                // Use bytes stream to support all file types
                InputStream in = new FileInputStream(src);
                OutputStream out = new FileOutputStream(dest);
    
                byte[] buffer = new byte[1024];
    
                int length;
                // copy the file content in bytes
                while ((length = in.read(buffer)) > 0)
                {
                    out.write(buffer, 0, length);
                }
    
                in.close();
                out.close();
            }
        } catch (IOException e)
        {   
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInstallAndUninstall() throws Throwable
    {
        Vagrant v = vagrant(Box.WINDOWS_2008_R2_AMD64, "win2008", "windows-2008R2-amd64-plain");
        v.up();
        try
        {

            v.copyFromHost(
                    System.getProperty("user.dir")
                            + "/target/classes/installer-windows-1.6-SNAPSHOT-windows-community.msi",
                    "/home/vagrant/installer.msi");

            CygwinShell sh = new CygwinShell(v.ssh());
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
            v.rollback(); // undo changes
        }
    }

    private void checkDataRest() throws Exception
    {
        JaxRsResponse r = RestRequest.req().get(
                "http://localhost:7474/db/data/");
        assertThat(r.getStatus(), equalTo(200));
    }
}
