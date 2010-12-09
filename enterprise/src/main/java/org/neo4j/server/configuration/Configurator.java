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
import java.util.Map;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.logging.Logger;

public class Configurator {

    public static final String DATABASE_LOCATION_PROPERTY_KEY = "org.neo4j.server.database.location";
    public static final String DEFAULT_CONFIG_DIR = File.separator + "etc" + File.separator + "neo";
    public static final String NEO_SERVER_CONFIG_FILE_KEY = "org.neo4j.server.properties";
    public static final String WEBSERVER_PORT_PROPERTY_KEY = "org.neo4j.server.webserver.port";
    public static final int DEFAULT_WEBSERVER_PORT = 7474;
    public static final String WEB_ADMIN_PATH = "/webadmin";
    public static final String WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION = "webadmin-html";
    public static final String WEB_ADMIN_REST_API_PATH = "/db/manage";
    public static final String WEB_ADMIN_REST_API_PACKAGE = "org.neo4j.server.webadmin.rest";
    public static final String REST_API_PATH = "/db/data";
    public static final String REST_API_PACKAGE = "org.neo4j.server.rest.web";
    public static final String ENABLE_OSGI_SERVER_PROPERTY_KEY = "org.neo4j.server.osgi.enable";
    public static final String OSGI_BUNDLE_DIR_PROPERTY_KEY = "org.neo4j.server.osgi.bundledir";
    public static final String OSGI_CACHE_DIR_PROPERTY_KEY = "org.neo4j.server.osgi.cachedir";
    public static final String WEBADMIN_NAMESPACE_PROPERTY_KEY = "org.neo4j.server.webadmin";
    public static final String WEB_ADMIN_PATH_PROPERTY_KEY = "org.neo4j.server.webadmin.management.uri";
    public static final String WEB_ADMIN_REST_API_PATH_PROPERTY_KEY = "org.neo4j.server.webadmin.data.uri";
    public static final String DB_TUNING_PROPERTY_FILE_KEY = "org.neo4j.server.db.tuning.properties";

    public static Logger log = Logger.getLogger(Configurator.class);

    private CompositeConfiguration serverConfiguration = new CompositeConfiguration();

    private Validator validator = new Validator();
    private Map<String, String> databaseTuningProperties = null;

    Configurator() {
        this(new Validator(), null);
    }

    public Configurator(File propertiesFile) {
        this(null, propertiesFile);
    }

    public Configurator(Validator v) {
        this(v, null);
    }

    public Configurator(Validator v, File propertiesFile) {
        if (propertiesFile == null) {
            propertiesFile = new File(System.getProperty(Configurator.NEO_SERVER_CONFIG_FILE_KEY));
        }

        try {
            loadPropertiesConfig(propertiesFile);
            if (v != null) {
                v.validate(this.configuration());
            }
        } catch (ConfigurationException ce) {
            log.warn(ce);
        }

    }

    public Configuration configuration() {
        return serverConfiguration == null ? new SystemConfiguration() : serverConfiguration;
    }

    private void loadPropertiesConfig(File configFile) throws ConfigurationException {
        PropertiesConfiguration propertiesConfig = new PropertiesConfiguration(configFile);
        if (validator.validate(propertiesConfig)) {
            serverConfiguration.addConfiguration(propertiesConfig);
            loadDatabaseTuningProperties();
        } else {
            String failed = String.format("Error processing [%s], configuration file has failed validation.", configFile.getAbsolutePath());
            log.fatal(failed);
            throw new InvalidServerConfigurationException(failed);
        }
    }

    private void loadDatabaseTuningProperties() {
        String databaseTuningPropertyFileLocation = serverConfiguration.getString(DB_TUNING_PROPERTY_FILE_KEY);
        
        if(databaseTuningPropertyFileLocation == null) {
            return;
        }
        
        if(!new File(databaseTuningPropertyFileLocation).exists()) {
            log.warn("The specified file for database performance tuning properties [%s] does not exist.", databaseTuningPropertyFileLocation);
            return;
        }

        databaseTuningProperties = EmbeddedGraphDatabase.loadConfigurations(databaseTuningPropertyFileLocation);
    }

    public Map<String, String> getDatabaseTuningProperties() {
        return databaseTuningProperties ;
    }
}
