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

import org.neo4j.vagrant.CygwinShell;
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
        cygSh.run(":> install.log");
        cygSh.runDOS("msiexec /quiet /L* install.log /i " + installerFileName + " INSTALL_DIR=\"C:\\"+WIN_INSTALL_FOLDER+"\"");
    }
    
    @Override
    public void runUninstall() {
        cygSh.run("net stop neo4j");
        cygSh.run(":> uninstall.log");
        cygSh.runDOS("msiexec /quiet /L* uninstall.log /x " + installerFileName);
    }

    @Override
    public void up() {
        super.up();
        cygSh = new CygwinShell(vm.ssh());
    }
    
    @Override
    public void startService() {
        sh.run("net start neo4j");
    }
    
    @Override
    public void stopService() {
        sh.run("net stop neo4j");
    }    
    
    @Override
    public String installDir() {
        return INSTALL_DIR;
    }
    
}
