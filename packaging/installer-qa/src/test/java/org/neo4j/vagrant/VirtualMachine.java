/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.vagrant;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.neo4j.vagrant.Shell.Result;

public class VirtualMachine {

    private Shell sh;
    private SSHConfig sshConfig;
    private boolean transactional = false;
    private VMDefinition definition;

    public VirtualMachine(File projectFolder, VMDefinition config)
    {
        this.sh = new Shell("host/" + config.vmName(), projectFolder);
        this.sh.getEnvironment().put("HOME", System.getProperty("user.home"));
        this.definition = config;
    }

    public void ensureBoxExists(Box box)
    {
        for (String boxName : vagrant("box list").getOutputAsList())
        {
            if (boxName.equals(box.getName()))
            {
                return;
            }
        }

        File oldWorkingDir = sh.getWorkingDir();
        try
        {
            sh.setWorkingDir(File.createTempFile("ignore","").getParentFile());
            vagrant("box add", box.getName(), box.getUrl());
            sh.setWorkingDir(oldWorkingDir);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void init(Box box)
    {
        vagrant("init", box.getName());
    }

    public void up()
    {
        vagrant("up");
        if(transactional) 
            vagrant("sandbox on");
    }

    public void halt()
    {
        vagrant("halt");
    }

    public void destroy()
    {
        vagrant("destroy");
    }

    public void reboot()
    {
        halt();
        up();
    }

    public SSHShell ssh()
    {
        return new SSHShell(definition().vmName(), sshConfiguration());
    }

    /**
     * Use SCP to move a file from the host to the VM.
     * 
     * This is really slow. If you are running a non-windows VM, opt for using
     * normal vagrant shared folders instead.
     * 
     * @param hostPath
     * @param vmPath
     * @return
     */
    public Result copyFromHost(String hostPath, String vmPath)
    {
        SSHConfig cfg = sshConfiguration();
        return scp(cfg.privateKeyPath(), hostPath, sshPath(cfg, vmPath),
                cfg.port());
    }

    /**
     * Use SCP to move a file to the host from the VM.
     * 
     * This is really slow. If you are running a non-windows VM, opt for using
     * normal vagrant shared folders instead.
     * 
     * @param hostPath
     * @param vmPath
     * @return
     */
    public Result copyFromVM(String vmPath, String hostPath)
    {
        SSHConfig cfg = sshConfiguration();
        return scp(cfg.privateKeyPath(), sshPath(cfg, vmPath), hostPath,
                cfg.port());
    }

    public SSHConfig sshConfiguration()
    {
        if (this.sshConfig == null)
        {
            this.sshConfig = SSHConfig.createFromVagrantOutput(vagrant(
                    "ssh-config").getOutputAsList());
        }

        return this.sshConfig;
    }

    public void commit()
    {
        if(transactional) {
            vagrant("sandbox commit");
            return;
        }
        throw new IllegalArgumentException("You have to create Vagrant object with transactional=true to use transactions.");
    }
    
    public void rollback()
    {
        if(transactional) {
            vagrant("sandbox rollback");
            return;
        }
        throw new IllegalArgumentException("You have to create Vagrant object with transactional=true to use transactions.");
    }

    public Shell getShell()
    {
        return sh;
    }
    
    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    public VMDefinition definition()
    {
        return definition;
    }

    protected Result vagrant(String... cmds)
    {

        Result r = sh.run("vagrant" + " " + StringUtils.join(cmds, " "));

        if (r.getExitCode() != 0)
        {
            throw new ShellException(r);
        }
        return r;
    }

    private Result scp(String privateKeyPath, String from, String to, int port)
    {
        from = escapeSshPathIfNecessary(from);
        to = escapeSshPathIfNecessary(to);
        
        Result r = sh.run("scp -i " + privateKeyPath
                + " -o StrictHostKeyChecking=no" + " -P " + port + " " + from
                + " " + to);
        if (r.getExitCode() != 0)
        {
            throw new ShellException(r);
        }

        return r;
    }

    private String escapeSshPathIfNecessary(String path)
    {
        if(path.contains(":") && path.contains("\\ ")) {
            return "\"" + path + "\"";
        }
        return path;
    }

    /*
     * user@host:port/path/path/path
     */
    private String sshPath(SSHConfig cfg, String path)
    {
        return cfg.user() + "@" + cfg.host() + ":" + path;
    }
}
