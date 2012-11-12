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

import java.util.List;

import org.neo4j.vagrant.VirtualMachine;

public interface Neo4jDriver {

    VirtualMachine vm();

    /**
     * Should install some edition of Neo4j.
     */
    void installNeo4j();
    
    /**
     * Should uninstall Neo4j.
     */
    void uninstallNeo4j();

    /**
     * Restart the VM
     */
    void reboot();

    /**
     * Boot this machine.
     */
    void up();

    /**
     * Stop the neo4j service.
     */
    void stopNeo4j();

    /**
     * Start the neo4j service.
     */
    void startNeo4j();

    /**
     * Delete the database
     */
    void deleteDatabase();

    /**
     * Close resources, like running
     * ssh session. Does not close the VM.
     */
    void close();

    /**
     * @return the directory where neo4j is installed.
     */
    String neo4jInstallDir();

    /**
     * Read a file.
     * @param string File path
     * @return
     */
    String readFile(String path);
    
    /**
     * Write a string to a file, overwriting
     * the file if it exists.
     * @param contents
     * @param path
     */
    void writeFile(String contents, String path);

    /**
     * List files in a directory
     * @param string Path
     * @return list of files
     */
    List<String> listDir(String string);

    /**
     * Modify a configuration file.
     * @param configFilePath
     * @param key
     * @param value
     */
    void setConfig(String configFilePath, String key, String value);

    /** 
     * Get a simple REST API client
     */
    Neo4jServerAPI neo4jClient();

    /**
     * Download relevant neo4j logs to the 
     * specified directory.
     * @param logDir
     */
    void downloadLogsTo(String logDir);
}
