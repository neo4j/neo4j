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

public class UbuntuDebEnterpriseDriver extends AbstractUbuntuDebDriver implements EnterpriseDriver {

    private static final String ZOOKEEPER_INSTALL_DIR = "/var/lib/neo4j-coordinator";
    private static final String BACKUP_DIR = "/home/vagrant";
    private String zookeeperInstallerPath;

    public UbuntuDebEnterpriseDriver(VirtualMachine vm, String installerPath, String zookeeperInstallerPath)
    {
        super(vm, installerPath);
        this.zookeeperInstallerPath = zookeeperInstallerPath;
    }
    
    @Override
    public void uninstallNeo4j() {
        sh.run("sudo dpkg -r neo4j-enterprise");
    }

    @Override
    public void installZookeeper()
    {
        vm.copyFromHost(zookeeperInstallerPath, "/home/vagrant/zookeeper.deb");
        sh.run("sudo dpkg -i /home/vagrant/zookeeper.deb");
        sh.run("sudo chmod -R 777 " + ZOOKEEPER_INSTALL_DIR + "/conf");
    }

    @Override
    public void uninstallZookeeper()
    {
        sh.run("sudo apt-get -qy remove zookeeper");
    }

    @Override
    public void startZookeeper()
    {
        sh.run("sudo /etc/init.d/neo4j-coord start");
    }

    @Override
    public void stopZookeeper()
    {
        sh.run("sudo /etc/init.d/neo4j-coord stop");
    }

    @Override
    public String zookeeperInstallDir()
    {
        return ZOOKEEPER_INSTALL_DIR;
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
        sh.run("sudo rm -rf " + neo4jInstallDir() + "/data/graph.db");
        sh.run("sudo mv " + BACKUP_DIR+"/"+backupName + " " + neo4jInstallDir() + "/data/graph.db");
        sh.run("sudo chown neo4j:adm -R " + neo4jInstallDir() + "/data/graph.db");
    }

    @Override
    public void downloadLogsTo(String target) {
        super.downloadLogsTo(target);
        String ip = vm().definition().ip();
        downloadLog(neo4jInstallDir() + "/data/log/neo4j-zookeeper.log", target + "/" + ip + "-neo4j-zookeeper-client.log");
        downloadLog(zookeeperInstallDir() + "/data/log/neo4j-zookeeper.log", target + "/" + ip + "-neo4j-zookeeper-server.log");
    }
    
    private void haBackup(String backupName, String coordinatorAddresses,
            String mode)
    {
        Result r = sh.run("cd " + neo4jInstallDir() + " && sudo chmod +x bin/neo4j-backup && sudo bin/neo4j-backup" + 
                " -" + mode +
                " -from ha://" + coordinatorAddresses +
                " -to " + BACKUP_DIR + "/" + backupName);
        if(!r.getOutput().contains("Done")) {
            throw new RuntimeException("Performing backup failed. Expected output to say 'Done', got this instead: \n" + r.getOutput());
        }
    }
}

