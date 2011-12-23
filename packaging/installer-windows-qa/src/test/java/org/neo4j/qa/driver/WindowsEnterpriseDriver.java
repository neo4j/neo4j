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

import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.VirtualMachine;


public class WindowsEnterpriseDriver extends AbstractWindowsDriver implements EnterpriseDriver {

    private static final String ZOOKEEPER_INSTALL_DIR = "zookeeper\\ with\\ space";
    private static final String ZOOKEEPER_WIN_INSTALL_DIR = "zookeeper with space";
    private static final String ZOOKEEPER_SERVICE = "Neo4jCoordinator";
    private String zookeeperInstallerPath;

    public WindowsEnterpriseDriver(VirtualMachine vm, String installerName, String zookeeperInstallerPath)
    {
        super(vm, installerName);
        this.zookeeperInstallerPath = zookeeperInstallerPath;
    }

    @Override
    public void runZookeeperInstall()
    {
        vm.copyFromHost(zookeeperInstallerPath, "/home/vagrant/zookeeper.msi");
        cygSh.run(":> zookeeper-install.log");
        cygSh.runDOS("msiexec /quiet /L* zookeeper-install.log /i zookeeper.msi INSTALL_DIR=\"C:\\"+ZOOKEEPER_WIN_INSTALL_DIR+"\"");
        if(!installIsSuccessful("/home/vagrant/zookeeper-install.log")){
            dumplog("/home/vagrant/zookeeper-install.log");
            dumplog(zookeeperInstallDir() + "/data/log/neo4j-zookeeper.log");
            throw new RuntimeException("Zookeeper install failed, dumped logs to stdout.");
        }
    }

    @Override
    public void runZookeeperUninstall()
    {
        cygSh.run(":> zookeeper-uninstall.log");
        cygSh.runDOS("msiexec /quiet /L* zookeeper-uninstall.log /x zookeeper.msi");
        if(!installIsSuccessful("/home/vagrant/zookeeper-uninstall.log")){
            dumplog("/home/vagrant/zookeeper-uninstall.log");
            dumplog(zookeeperInstallDir() + "/data/log/neo4j-zookeeper.log");
            throw new RuntimeException("Zookeeper uninstall failed, dumped logs to stdout.");
        }
    }

    @Override
    public void startZookeeperService()
    {
        Result r = sh().run("net start " + ZOOKEEPER_SERVICE);
        if(!r.getOutput().contains("service was started successfully")) {
            dumplog(installDir() + "/data/log/neo4j.0.0.log");
            dumplog(zookeeperInstallDir() + "/data/log/neo4j-zookeeper.log");
            throw new RuntimeException("Tried to start neo4j coordinator, failed. Output was: \n" + r.getOutput());
        }
    }

    @Override
    public void stopZookeeperService()
    {
        Result r = sh().run("net stop " + ZOOKEEPER_SERVICE);
        if(!r.getOutput().contains("service was stopped successfully")) {
            dumplog(installDir() + "/data/log/neo4j.0.0.log");
            dumplog(zookeeperInstallDir() + "/data/log/neo4j-zookeeper.log");
            throw new RuntimeException("Tried to stop neo4j coordinator, failed. Output was: \n" + r.getOutput());
        }
    }

    @Override
    public String zookeeperInstallDir()
    {
        return "/cygdrive/c/" + ZOOKEEPER_INSTALL_DIR;
    }


}
