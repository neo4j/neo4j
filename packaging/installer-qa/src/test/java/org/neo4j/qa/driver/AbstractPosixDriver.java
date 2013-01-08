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

import java.util.List;

import org.neo4j.vagrant.SSHShell;
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.VirtualMachine;

public abstract class AbstractPosixDriver implements Neo4jDriver {

    protected VirtualMachine vm;
    private SSHShell sh;
    private Neo4jServerAPI serverAPI;

    public AbstractPosixDriver(VirtualMachine vm)
    {
        this.vm = vm;
    }

    @Override
    public void close() {
        if(sh != null) {
            sh.close();
            sh = null;
        }
    }   

    @Override
    public VirtualMachine vm()
    {
        return vm;
    }

    @Override
    public void up() {
        vm.up();
    }
    
    @Override
    public void reboot() {
        close();
        vm.halt();
        up();
    }
    
    @Override
    public void deleteDatabase() {
        sh("sudo rm -rf " + neo4jInstallDir() + "/data/graph.db");
    }

    @Override
    public Neo4jServerAPI neo4jClient() {
        if(serverAPI == null) {
            serverAPI = new Neo4jServerAPI("http://" + vm().definition().ip() + ":7474");
        }
        return serverAPI;
    }
    
    //
    // File management
    //
    
    @Override
    public String readFile(String path) {
        return sh("cat", path).getOutput();
    }
    
    @Override
    public List<String> listDir(String path) {
        return sh("ls", path).getOutputAsList();
    }
    
    @Override
    public void writeFile(String contents, String path) {
        sh("echo '"+contents+"' | sudo tee " + path + " > /dev/null");
    }

    @Override
    public void setConfig(String configFilePath, String key, String value) {
        // Remove any pre-existing config directive for this key, then append
        // the new setting at the bottom of the file.
        sh("sudo sed -i 's/^"+key+"=.*//g' "+configFilePath+" && sudo echo " + key + "=" + value + " >> " + configFilePath);
    }
    
    protected Result sh(String ... cmds) {
        if(sh == null) {
            sh = vm.ssh();
        }
        return sh.run(cmds);
    }
    
}
