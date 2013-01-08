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
package org.neo4j.qa.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.neo4j.qa.SharedConstants;
import org.neo4j.vagrant.CygwinShell;
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.VirtualMachine;

public class WindowsCommunityDriver extends AbstractPosixDriver {

    private static final String TEMP_DOWNLOAD_LOG_PATH = "/home/vagrant/tobedownloaded.log";
    protected static final String WIN_INSTALL_DIR = "C:\\neo4j install with spaces";
    private static final String INSTALL_FOLDER = "neo4j\\ install\\ with\\ spaces";
    private static final String INSTALL_DIR = "/cygdrive/c/" + INSTALL_FOLDER;
    
    protected String installerPath;
    protected String installerFileName;
    private CygwinShell cygSh;

    public WindowsCommunityDriver(VirtualMachine vm, String installerPath) {
        super(vm);
        this.installerPath = installerPath;
        this.installerFileName = new File(installerPath).getName();
    }
    
    public WindowsCommunityDriver(VirtualMachine vm)
    {
        this(vm, SharedConstants.WINDOWS_COMMUNITY_INSTALLER );
    }

    @Override
    public void close() {
        if(cygSh != null) {
            cygSh.close();
            cygSh = null;
        }
        super.close();
    } 
    
    @Override
    public void installNeo4j() {
        vm.copyFromHost(installerPath, "/home/vagrant/" + installerFileName);
        sh("touch install.log");
        bash("msiexec /quiet /L* install.log /i " + installerFileName + " INSTALL_DIR=\""+WIN_INSTALL_DIR+"\"");
        
        if(!installIsSuccessful("/home/vagrant/install.log")) {
            throw new RuntimeException("Failed to install neo4j ["+vm().definition().ip()+"], see build log.");
        }
    }
    
    @Override
    public void uninstallNeo4j() {
        sh("net stop neo4j");
        sh(":> uninstall.log");
        bash("msiexec /quiet /L* uninstall.log /x " + installerFileName);
        
        if(!installIsSuccessful("/home/vagrant/uninstall.log")) {
            throw new RuntimeException("Failed to uninstall neo4j ["+vm().definition().ip()+"], see build log.");
        }
    }
    
    @Override
    public void startNeo4j() {
        Result r = sh("net start neo4j");
        if(!r.getOutput().contains("service was started successfully")) {
            throw new RuntimeException("Tried to start neo4j ["+vm().definition().ip()+"], failed. Output was: \n" + r.getOutput());
        }
    }
    
    @Override
    public void stopNeo4j() {
        Result r = sh("net stop neo4j");
        if(!r.getOutput().contains("service was stopped successfully")) {
            throw new RuntimeException("Tried to stop neo4j ["+vm().definition().ip()+"], failed. Output was: \n" + r.getOutput());
        }
    }    
    
    @Override
    public String neo4jInstallDir() {
        return INSTALL_DIR;
    }
    
    @Override
    public void downloadLogsTo(String target) {
        String ip = vm().definition().ip();
        
        downloadLog(neo4jInstallDir() + "/data/graph.db/messages.log", target + "/" + ip + "-messages.log");
        downloadLog(neo4jInstallDir() + "/data/log/neo4j.0.0.log", target + "/" + ip + "-neo4j.0.0.log");
        downloadLog("/home/vagrant/install.log", target + "/" + ip + "-install.log");
        downloadLog("/home/vagrant/uninstall.log", target + "/" + ip + "-uninstall.log");
        downloadLog(neo4jInstallDir() + "/conf/neo4j-server.properties", target + "/" + ip + "-neo4j-server.properties");
        downloadLog(neo4jInstallDir() + "/conf/neo4j.properties", target + "/" + ip + "-neo4j.properties");
    }

    @Override
    public void writeFile(String contents, String path) {
        sh("echo '"+contents+"' > " + path);
    }
    
    protected void downloadLog(String from, String to)
    {
        if( ! sh("ls " + from).getOutput().contains("No such file")) {
            sh("rm " + TEMP_DOWNLOAD_LOG_PATH);
            sh("cp " + from + " " + TEMP_DOWNLOAD_LOG_PATH);
            vm().copyFromVM(TEMP_DOWNLOAD_LOG_PATH, to);
        } else {
            try
            {
                FileUtils.writeStringToFile(new File(to), "This log file did not exist on the VM.");
            } catch (IOException e1)
            {   
                throw new RuntimeException(e1);
            }
        }
    }
    
    protected boolean installIsSuccessful(String logPath) {
        String log = readInstallationLog(logPath);
        return log.contains("success or error status: 0");
    }
    
    @Override
    protected Result sh(String ... commands) {
        
        return cygSh().run(commands);
    }
    
    protected Result bash(String ... commands) {
        
        return cygSh().runDOS(commands);
    }
    
    private CygwinShell cygSh() {
        if(cygSh == null) {
            cygSh = new CygwinShell(vm.ssh());
        }
        return cygSh;
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
