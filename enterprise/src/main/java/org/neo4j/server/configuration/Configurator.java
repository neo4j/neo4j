/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

public class Configurator {

    private File defaultConfigurationDirectory = new File("etc" + File.separatorChar + "neo-server");
    private CompositeConfiguration serverConfiguration = new CompositeConfiguration();

    public Configurator(File... configDirs) {
        if (configDirs == null || configDirs.length == 0) {
            configDirs = new File[1];
            configDirs[0] = defaultConfigurationDirectory;
        }
        
        for (File configDir : configDirs) {
            loadConfigFrom(configDir);
        }
    }

    public Configuration configuration() {
        return serverConfiguration == null ? new SystemConfiguration() : serverConfiguration;
    }

    private void loadConfigFrom(File configDir) {
        
        if(configDir.exists()) {
            loadXmlConfig(configDir);
            loadPropertiesConfig(configDir);           
        }
    }

    private void loadPropertiesConfig(File configDir) {

        for (File configFile : getCandidateConfigFiles(configDir, ".properties")) {
            try {
                PropertiesConfiguration propertiesConfig = new PropertiesConfiguration(configFile);
                serverConfiguration.addConfiguration(propertiesConfig);
            } catch (Exception e) {
                logFailureToLoadConfigFile(configFile, e);
            }
        }
    }

    private void loadXmlConfig(File configDir) {
        for (File configFile : getCandidateConfigFiles(configDir, ".xml")) {
            try {
                XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
                serverConfiguration.addConfiguration(xmlConfig);
            } catch (Exception e) {
                logFailureToLoadConfigFile(configFile, e);
            }
        }
    }

    private void logFailureToLoadConfigFile(File configFile, Exception e) {
        Logger.getLogger(this.getClass().toString()).log(Level.INFO,
                String.format("The configuration file [%s] could not be loaded as a property file.", configFile.getAbsolutePath()));
        Logger.getLogger(this.getClass().toString()).log(Level.INFO, e.getMessage());
   }
    
    private File[] getCandidateConfigFiles(final File configDir, final String fileExtension) {
        FilenameFilter filenameFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(fileExtension);
            }
        };
        
        File[] listFiles = configDir.listFiles(filenameFilter);
       
        if(listFiles == null) {
            listFiles = new File[0];
        }
        
        return listFiles;
    }
}
