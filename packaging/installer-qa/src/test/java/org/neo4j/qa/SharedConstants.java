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
package org.neo4j.qa;

public class SharedConstants {

    public static final String NEO4J_VERSION = System.getProperty(
            "neo4j.version", "");
    
    public static final String INSTALLER_DIR = System.getProperty(
            "neo4j.installer-dir", System.getProperty("user.dir")
                    + "/target/classes/");

    public static final String TEST_LOGS_DIR = System.getProperty(
            "neo4j.installer-dir", System.getProperty("user.dir")
                    + "/target/test-logs/");
    
    public static final String WINDOWS_COMMUNITY_INSTALLER  = INSTALLER_DIR + "installer-windows-community.msi";
    public static final String WINDOWS_ADVANCED_INSTALLER   = INSTALLER_DIR + "installer-windows-advanced.msi";
    public static final String WINDOWS_ENTERPRISE_INSTALLER = INSTALLER_DIR + "installer-windows-enterprise.msi";
    public static final String WINDOWS_COORDINATOR_INSTALLER  = INSTALLER_DIR + "installer-windows-coordinator.msi";
    
    public static final String DEBIAN_COMMUNITY_INSTALLER = INSTALLER_DIR + "installer-debian-community.deb";
    public static final String DEBIAN_ADVANCED_INSTALLER = INSTALLER_DIR + "installer-debian-advanced.deb";
    public static final String DEBIAN_ENTERPRISE_INSTALLER = INSTALLER_DIR + "installer-debian-enterprise.deb";
    public static final String DEBIAN_COORDINATOR_INSTALLER  = INSTALLER_DIR + "installer-debian-coordinator.deb";
    
    public static final String UNIX_COMMUNITY_TARBALL = INSTALLER_DIR + "installer-unix-community.tar.gz";
    public static final String UNIX_ADVANCED_TARBALL = INSTALLER_DIR + "installer-unix-advanced.tar.gz";
    public static final String UNIX_ENTERPRISE_TARBALL = INSTALLER_DIR + "installer-unix-enterprise.tar.gz";
    public static final String UNIX_COORDINATOR_TARBALL  = INSTALLER_DIR + "installer-unix-coordinator.tar.gz";
    
}
