/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.logging.Logger;

public class Configurator {
    public static final String DB_TUNING_PROPERTY_FILE_KEY = "org.neo4j.server.db.tuning.properties";
    public static final String DEFAULT_CONFIG_DIR = File.separator + "etc" + File.separator + "neo";
    public static final String DATABASE_LOCATION_PROPERTY_KEY = "org.neo4j.server.database.location";
    public static final String NEO_SERVER_CONFIG_FILE_KEY = "org.neo4j.server.properties";
    public static final String DB_MODE_KEY = "org.neo4j.server.database.mode";

    public static final int DEFAULT_WEBSERVER_PORT = 7474;
    public static final String WEBSERVER_PORT_PROPERTY_KEY = "org.neo4j.server.webserver.port";
    public static final String DEFAULT_WEB_ADMIN_PATH = "/webadmin";
    public static final String DEFAULT_WEB_ADMIN_STATIC_WEB_CONTENT_LOCATION = "webadmin-html";

    public static final String REST_API_PACKAGE = "org.neo4j.server.rest.web";
    public static final String ENABLE_OSGI_SERVER_PROPERTY_KEY = "org.neo4j.server.osgi.enable";
    public static final String OSGI_BUNDLE_DIR_PROPERTY_KEY = "org.neo4j.server.osgi.bundledir";
    public static final String OSGI_CACHE_DIR_PROPERTY_KEY = "org.neo4j.server.osgi.cachedir";
    public static final String WEB_ADMIN_REST_API_PACKAGE = "org.neo4j.server.webadmin.rest";
    public static final String WEBADMIN_NAMESPACE_PROPERTY_KEY = "org.neo4j.server.webadmin";
    public static final String WEB_ADMIN_PATH_PROPERTY_KEY = "org.neo4j.server.webadmin.management.uri";
    public static final String REST_API_PATH_PROPERTY_KEY = "org.neo4j.server.webadmin.data.uri";
    public static final String THIRD_PARTY_PACKAGES_KEY = "org.neo4j.server.thirdparty_jaxrs_classes";

    public static Logger log = Logger.getLogger(Configurator.class);

    private CompositeConfiguration serverConfiguration = new CompositeConfiguration();

    private Validator validator = new Validator();
    private Map<String, String> databaseTuningProperties = null;
    private HashSet<ThirdPartyJaxRsPackage> thirdPartyPackages;

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
            normalizeUris();
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

    private void normalizeUris() {
        try {
            for (String key : new String[] { WEB_ADMIN_PATH_PROPERTY_KEY, REST_API_PATH_PROPERTY_KEY }) {
                if (configuration().containsKey(key)) {
                    URI normalizedUri = makeAbsoluteAndNormalized(new URI((String) configuration().getProperty(key)));
                    configuration().clearProperty(key);
                    configuration().addProperty(key, normalizedUri.toString());
                }
            }

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

    }

    private URI makeAbsoluteAndNormalized(URI uri) {
        if (uri.isAbsolute())
            return uri.normalize();

        String portNo = (String) configuration().getProperty(WEBSERVER_PORT_PROPERTY_KEY);
        if (portNo == null)
            portNo = "80";

        StringBuilder sb = new StringBuilder();
        sb.append("http://localhost");
        if (portNo != "80") {
            sb.append(":");
            sb.append(portNo);
        }
        sb.append("/");
        sb.append(uri.toString());
        try {
            return new URI(sb.toString()).normalize();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadDatabaseTuningProperties() {
        String databaseTuningPropertyFileLocation = serverConfiguration.getString(DB_TUNING_PROPERTY_FILE_KEY);

        if (databaseTuningPropertyFileLocation == null) {
            return;
        }

        File databaseTuningPropertyFile = new File(databaseTuningPropertyFileLocation);
        
        if (!databaseTuningPropertyFile.exists()) {
            log.warn("The specified file for database performance tuning properties [%s] does not exist.", databaseTuningPropertyFileLocation);
            return;
        }

        databaseTuningProperties = EmbeddedGraphDatabase.loadConfigurations(databaseTuningPropertyFile.getAbsolutePath());
    }

    public Map<String, String> getDatabaseTuningProperties() {
        return databaseTuningProperties;
    }

    public Set<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses() {
        thirdPartyPackages = new HashSet<ThirdPartyJaxRsPackage>();
        Properties properties = this.configuration().getProperties(THIRD_PARTY_PACKAGES_KEY);
        for (Object key : properties.keySet()) {
            thirdPartyPackages.add(new ThirdPartyJaxRsPackage(key.toString(), properties.getProperty(key.toString())));
        }
        return thirdPartyPackages;
    }
}
