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
import java.util.Iterator;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.neo4j.server.configuration.validation.DuplicateKeyRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.logging.Logger;

public class Configurator {

    public static Logger log = Logger.getLogger(Configurator.class);

    private File defaultConfigurationDirectory = new File("etc" + File.separatorChar + "neo-server");
    private CompositeConfiguration serverConfiguration = new CompositeConfiguration();

    private Validator validator = new Validator(new DuplicateKeyRule());

    public Configurator() {
        this(null);
    }

    public Configurator(File configDir) {
        if (configDir == null) {
            configDir = defaultConfigurationDirectory;
        }

        try {
            loadConfigFrom(configDir);
        } catch(ConfigurationException ce) {
            log.warn(ce);
        }

    }

    public Configuration configuration() {
        return serverConfiguration == null ? new SystemConfiguration() : serverConfiguration;
    }

    private void loadConfigFrom(File configDir) throws ConfigurationException {

        if (configDir.exists() && configDir.isDirectory()) {
            loadXmlConfig(configDir);
            loadPropertiesConfig(configDir);
        }
    }

    private void loadPropertiesConfig(File configDir) throws ConfigurationException {

        for (File configFile : getCandidateConfigFiles(configDir, ".properties")) {

            PropertiesConfiguration propertiesConfig = new PropertiesConfiguration(configFile);
            if (validator.validate(serverConfiguration, propertiesConfig)) {
                serverConfiguration.addConfiguration(propertiesConfig);
            } else {
                String failed = String.format("Error processing [%s], configuration file(s) corrupt or contains duplicates", configFile.getAbsolutePath());
                log.fatal(failed);
                throw new InvalidServerConfigurationException(failed);
            }
        }
    }

    private void loadXmlConfig(File configDir) throws ConfigurationException {
        for (File configFile : getCandidateConfigFiles(configDir, ".xml")) {

            XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
            if (validator.validate(serverConfiguration, xmlConfig)) {
                serverConfiguration.addConfiguration(xmlConfig);
            }
        }
    }

    private File[] getCandidateConfigFiles(final File configDir, final String fileExtension) {
        FilenameFilter filenameFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(fileExtension);
            }
        };

        File[] listFiles = configDir.listFiles(filenameFilter);

        if (listFiles == null) {
            listFiles = new File[0];
        }

        return listFiles;
    }
}
