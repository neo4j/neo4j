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
package org.neo4j.qa.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.neo4j.vagrant.CygwinShell;
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.VirtualMachine;

/*
 * We extend AbstractPosixDriver, because we use Cygwin to talk to windows.
 */
public abstract class AbstractWindowsDriver extends AbstractPosixDriver {

    private static final String WIN_INSTALL_FOLDER = "neo4j install with spaces";
    private static final String INSTALL_FOLDER = "neo4j\\ install\\ with\\ spaces";
    private static final String INSTALL_DIR = "/cygdrive/c/" + INSTALL_FOLDER;
    
    protected String installerPath;
    protected String installerFileName;
    protected CygwinShell cygSh;

    public AbstractWindowsDriver(VirtualMachine vm, String installerPath) {
        super(vm);
        this.installerPath = installerPath;
        this.installerFileName = new File(installerPath).getName();
    }
    
    @Override
    public void runInstall() {
        vm.copyFromHost(installerPath, "/home/vagrant/" + installerFileName);
        cygSh.run("touch install.log");
        cygSh.runDOS("msiexec /quiet /L* install.log /i " + installerFileName + " INSTALL_DIR=\"C:\\"+WIN_INSTALL_FOLDER+"\"");
        
        if(!installIsSuccessful("/home/vagrant/install.log")) {
            dumplog("/home/vagrant/install.log");
            dumplog(installDir() + "/data/log/neo4j.0.0.log");
            throw new RuntimeException("Failed to install neo4j, see build log.");
        }
    }
    
    @Override
    public void runUninstall() {
        cygSh.run("net stop neo4j");
        cygSh.run(":> uninstall.log");
        cygSh.runDOS("msiexec /quiet /L* uninstall.log /x " + installerFileName);
        
        if(!installIsSuccessful("/home/vagrant/uninstall.log")) {
            dumplog("/home/vagrant/uninstall.log");
            dumplog(installDir() + "/data/log/neo4j.0.0.log");
            throw new RuntimeException("Failed to uninstall neo4j, see build log.");
        }
    }

    @Override
    public void up() {
        super.up();
        cygSh = new CygwinShell(vm.ssh());
    }
    
    @Override
    public void startService() {
        Result r = sh.run("net start neo4j");
        if(!r.getOutput().contains("service was started successfully")) {
            dumplog(installDir() + "/data/log/neo4j.0.0.log");
            throw new RuntimeException("Tried to start neo4j, failed. Output was: \n" + r.getOutput());
        }
    }
    
    @Override
    public void stopService() {
        Result r = sh.run("net stop neo4j");
        if(!r.getOutput().contains("service was stopped successfully")) {
            dumplog(installDir() + "/data/log/neo4j.0.0.log");
            throw new RuntimeException("Tried to stop neo4j, failed. Output was: \n" + r.getOutput());
        }
    }    
    
    @Override
    public String installDir() {
        return INSTALL_DIR;
    }
    
    protected void dumplog(String logPath) {
        System.out.println("Dumping log: " + logPath);
        System.out.println();
        sh.run("cat " + logPath);
        System.out.println();
    }
    
    protected boolean installIsSuccessful(String logPath) {
        String log = readInstallationLog(logPath);
        return log.contains("success or error status: 0");
    }
    
    private String readInstallationLog(String path) {
        try
        {   int c;
            File f = File.createTempFile("install", "log");
            
            vm.copyFromVM(path, f.getAbsolutePath());
            
            FileInputStream in = new FileInputStream(f);
            StringBuilder b = new StringBuilder();
            
            in.skip(2); // Windows puts "FF FE" at the beginning of the file.
            
            while((c = in.read()) != -1) {
                b.append((char)c);
                in.skip(1); // Every other byte is null
            }
            return b.toString();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
}
