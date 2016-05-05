/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config;

import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import org.neo4j.desktop.config.portable.Environment;

/**
 * The Installation represents the "static" part of the configuration on a particular system. It abstracts away
 * operating system specifics.
 */
public interface Installation
{
    String NEO4J_CONFIG_FILENAME = "neo4j.conf";
    String NEO4J_VMOPTIONS_FILENAME = "neo4j-community.vmoptions";
    String DEFAULT_CONFIG_RESOURCE_NAME = "/org/neo4j/desktop/config/neo4j-default.conf";
    String DEFAULT_VMOPTIONS_TEMPLATE_RESOURCE_NAME = "/org/neo4j/desktop/config/vmoptions.template";
    String INSTALL_PROPERTIES_FILENAME = "install.properties";

    /**
     * Get a facade for interacting with the environment, such as opening file editors and browsing URLs.
     */
    Environment getEnvironment();

    /**
     * Get the directory wherein the database will put its store files.
     */
    File getDatabaseDirectory();

    /**
     * Get the directory where the configuration properties files are located.
     */
    File getConfigurationDirectory();

    /**
     * Get the abstract path name that points to the neo4j-community.vmoptions file.
     */
    File getVmOptionsFile();

    /**
     * Get the abstract path name that points to the neo4j.conf file.
     */
    File getConfigurationsFile();

    /**
     * Initialize the installation, such that we make sure that the various configuration files
     * exist where we expect them to.
     */
    void initialize() throws Exception;

    /**
     * Get the contents for a default neo4j.conf file.
     */
    InputStream getDefaultDatabaseConfiguration();

    /**
     * Get the contents for a default neo4j-community.vmoptions file.
     */
    InputStream getDefaultVmOptions();

    /**
     * Get the directory into which Neo4j Desktop has been installed.
     */
    File getInstallationDirectory() throws URISyntaxException;

    /**
     * Get the directory where the neo4j-desktop.jar file has been installed into.
     */
    File getInstallationBinDirectory() throws URISyntaxException;

    /**
     * Get the directory where bundled JRE binaries are located.
     */
    File getInstallationJreBinDirectory() throws URISyntaxException;
}
