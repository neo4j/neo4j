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

import org.neo4j.qa.SharedConstants;
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.VirtualMachine;


public class WindowsEnterpriseDriver extends WindowsCommunityDriver implements EnterpriseDriver {

    private static final String BACKUP_DIR_NAME = "backups";
    private static final String ZOOKEEPER_INSTALL_DIR = "zookeeper\\ with\\ space";
    private static final String ZOOKEEPER_WIN_INSTALL_DIR = "zookeeper with space";
    private static final String ZOOKEEPER_SERVICE = "Neo4jCoordinator";
    private String zookeeperInstallerPath;

    public WindowsEnterpriseDriver(VirtualMachine vm, String installerName, String zookeeperInstallerPath)
    {
        super(vm, installerName);
        this.zookeeperInstallerPath = zookeeperInstallerPath;
    }

    public WindowsEnterpriseDriver(VirtualMachine vm)
    {
        this(vm, 
            SharedConstants.WINDOWS_ENTERPRISE_INSTALLER, 
            SharedConstants.WINDOWS_COORDINATOR_INSTALLER);
    }

    @Override
    public void installZookeeper()
    {
        vm.copyFromHost(zookeeperInstallerPath, "/home/vagrant/zookeeper.msi");
        sh(":> zookeeper-install.log");
        bash("msiexec /quiet /L* zookeeper-install.log /i zookeeper.msi INSTALL_DIR=\"C:\\"+ZOOKEEPER_WIN_INSTALL_DIR+"\"");
        if(!installIsSuccessful("/home/vagrant/zookeeper-install.log")){
            throw new RuntimeException("Zookeeper install failed ["+vm().definition().ip()+"].");
        }
    }

    @Override
    public void uninstallZookeeper()
    {
        sh(":> zookeeper-uninstall.log");
        bash("msiexec /quiet /L* zookeeper-uninstall.log /x zookeeper.msi");
        if(!installIsSuccessful("/home/vagrant/zookeeper-uninstall.log")){
            throw new RuntimeException("Zookeeper uninstall failed ["+vm().definition().ip()+"].");
        }
    }

    @Override
    public void startZookeeper()
    {
        Result r = sh("net start " + ZOOKEEPER_SERVICE);
        if(!r.getOutput().contains("service was started successfully")) {
            throw new RuntimeException("Tried to start neo4j coordinator ["+vm().definition().ip()+"], failed. Output was: \n" + r.getOutput());
        }
    }

    @Override
    public void stopZookeeper()
    {
        Result r = sh("net stop " + ZOOKEEPER_SERVICE);
        if(!r.getOutput().contains("service was stopped successfully") &&
           !r.getOutput().contains("service is not started.")) {
            throw new RuntimeException("Tried to stop neo4j coordinator ["+vm().definition().ip()+"], failed. Output was: \n" + r.getOutput());
        }
    }

    @Override
    public String zookeeperInstallDir()
    {
        return "/cygdrive/c/" + ZOOKEEPER_INSTALL_DIR;
    }

    @Override
    public void performFullHABackup(String backupName, String coordinatorAddresses)
    {
        haBackup(backupName, coordinatorAddresses, "full");
    }

    @Override
    public void performIncrementalHABackup(String backupName,
            String coordinatorAddresses)
    {
        haBackup(backupName, coordinatorAddresses, "incremental");
    }

    @Override
    public void replaceGraphDataDirWithBackup(String backupName)
    {
        sh("rm -rf " + neo4jInstallDir() + "/data/graph.db");
        sh("mv " + neo4jInstallDir()+"/"+BACKUP_DIR_NAME+"/"+backupName + " " + neo4jInstallDir() + "/data/graph.db");
    }

    @Override
    public void downloadLogsTo(String target) {
        super.downloadLogsTo(target);
        String ip = vm().definition().ip();
        downloadLog(neo4jInstallDir() + "/data/log/neo4j-zookeeper.log", target + "/" + ip + "-neo4j-zookeeper-client.log");
        downloadLog(zookeeperInstallDir() + "/data/log/neo4j-zookeeper.log", target + "/" + ip + "-neo4j-zookeeper-server.log");
        downloadLog("/home/vagrant/zookeeper-install.log", target + "/" + ip + "-zookeeper-install.log");
        downloadLog("/home/vagrant/zookeeper-uninstall.log", target + "/" + ip + "-zookeeper-uninstall.log");
    }
    
    private void haBackup(String backupName, String coordinatorAddresses,
            String mode)
    {
        Result r = sh("cd " + neo4jInstallDir() + " && bin/Neo4jBackup.bat" + 
                " -" + mode +
                " -from ha://" + coordinatorAddresses +
                " -to " + BACKUP_DIR_NAME + "/" + backupName);
        if(!r.getOutput().contains("Done")) {
            throw new RuntimeException("Performing backup failed. Expected output to say 'Done', got this instead: \n" + r.getOutput());
        }
    }


}
