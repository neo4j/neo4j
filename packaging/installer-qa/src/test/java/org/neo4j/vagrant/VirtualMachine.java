/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.io.PrintWriter;

import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.command.AddBox;
import org.neo4j.vagrant.command.Commit;
import org.neo4j.vagrant.command.Destroy;
import org.neo4j.vagrant.command.EnableSandbox;
import org.neo4j.vagrant.command.GetSshConfig;
import org.neo4j.vagrant.command.GetState;
import org.neo4j.vagrant.command.GetState.VirtualMachineState;
import org.neo4j.vagrant.command.Halt;
import org.neo4j.vagrant.command.Init;
import org.neo4j.vagrant.command.ListBoxes;
import org.neo4j.vagrant.command.Rollback;
import org.neo4j.vagrant.command.Up;
import org.neo4j.vagrant.command.VagrantExecutor;

public class VirtualMachine {

    private static final String DEFAULT_SCP_PATH = "scp";
    private static final String DEFAULT_VAGRANT_PATH = "vagrant";

    private static final String SCP_PATH_KEY = "scp.path";
    private static final String VAGRANT_PATH_KEY = "vagrant.path";
    private static final int MAX_COMMAND_RETRIES = 5;
    
    private Shell sh;
    private SSHConfig sshConfig;
    private boolean transactional = false;
    private VMDefinition definition;

    private String scpPath;
    private VagrantExecutor vagrant;
    
    // TODO: Refactor how logging is passed to VirtualMachine
    // such that we get one log of all commands on all servers *per test*.
    private PrintWriter vmShellLog;
    private PrintWriter hostShellLog;
    
    public VirtualMachine(File projectFolder, VMDefinition config, PrintWriter hostLog, PrintWriter vmLog)
    {
        this(projectFolder, 
             config, 
             hostLog,
             vmLog,
             System.getProperty(VAGRANT_PATH_KEY, DEFAULT_VAGRANT_PATH), 
             System.getProperty(SCP_PATH_KEY, DEFAULT_SCP_PATH));
    }
    
    private VirtualMachine(File projectFolder, VMDefinition config, PrintWriter hostLog, PrintWriter vmLog, String vagrantPath, String scpPath)
    {
    	this.hostShellLog = hostLog;
    	this.vmShellLog = vmLog;
    	
        this.sh = new Shell("host/" + config.vmName(), projectFolder, hostShellLog);
        this.sh.getEnvironment().put("HOME", System.getProperty("user.home"));
        this.definition = config;
        this.scpPath = scpPath;
        this.vagrant = new VagrantExecutor(sh, vagrantPath, MAX_COMMAND_RETRIES);
    }

    public void ensureBoxExists(Box box)
    {
        for (String boxName : vagrant.execute(new ListBoxes()))
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
            
            vagrant.execute(new AddBox(box));
            
            sh.setWorkingDir(oldWorkingDir);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void init()
    {
        vagrant.execute(new Init(definition.box().getName()));
    }

    public void up()
    {
        vagrant.execute(new Up());
        if(transactional) 
            vagrant.execute(new EnableSandbox(true));
    }

    public void halt()
    {
        vagrant.execute(new Halt());
    }

    public void destroy()
    {
        vagrant.execute(new Destroy());
    }

    public void reboot()
    {
        halt();
        up();
    }
    
    public void commit()
    {
        if(transactional) {
            vagrant.execute(new Commit());
            return;
        }
        throw new IllegalArgumentException("You have to create Vagrant object with transactional=true to use transactions.");
    }
    
    public void rollback()
    {
        if(transactional) {
            vagrant.execute(new Rollback());
            return;
        }
        throw new IllegalArgumentException("You have to create Vagrant object with transactional=true to use transactions.");
    }
    
    public VirtualMachineState state() {
        return vagrant.execute(new GetState());
    }

    public SSHShell ssh()
    {
        return new SSHShell(definition().vmName(), sshConfiguration(), vmShellLog);
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
            this.sshConfig = vagrant.execute(new GetSshConfig());
        }

        return this.sshConfig;
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

    private Result scp(String privateKeyPath, String from, String to, int port)
    {
        File tmpHostsFile = null;
        try {
            from = escapeSshPathIfNecessary(from);
            to = escapeSshPathIfNecessary(to);
            
            tmpHostsFile = File.createTempFile("known-hosts", "throwaway");
            
            Result r = sh.run(scpPath 
                    + " -i " + privateKeyPath
                    + " -o StrictHostKeyChecking=no"
                    + " -o UserKnownHostsFile=" + tmpHostsFile.getAbsolutePath()
                    + " -P " + port
                    + " " + from
                    + " " + to);
            if (r.getExitCode() != 0)
            {
                throw new ShellException(r);
            }
    
            return r;
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(tmpHostsFile != null) 
                tmpHostsFile.delete();
        }
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
