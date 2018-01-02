/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.configuration;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.TimeUtil;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.logging.Logger;
import org.neo4j.server.web.ServerInternalSettings;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Using the server settings from {@link ServerSettings} with {@link Configuration a configuration}.
 */
@Deprecated
public interface Configurator
{
    String SECURITY_RULES_KEY = ServerSettings.security_rules.name();

    String AUTH_STORE_FILE_KEY = ServerInternalSettings.auth_store.name();
    String DB_TUNING_PROPERTY_FILE_KEY = ServerInternalSettings.legacy_db_config.name();
    String DEFAULT_CONFIG_DIR = File.separator + "etc" + File.separator + "neo";
    String DATABASE_LOCATION_PROPERTY_KEY = ServerInternalSettings.legacy_db_location.name();
    String DEFAULT_DATABASE_LOCATION_PROPERTY_KEY = ServerInternalSettings.legacy_db_location.getDefaultValue();

    String NEO_SERVER_CONFIG_FILE_KEY = ServerInternalSettings.SERVER_CONFIG_FILE_KEY;
    String DB_MODE_KEY = "org.neo4j.server.database.mode";

    int DEFAULT_WEBSERVER_PORT = Integer.valueOf( ServerSettings.webserver_port.getDefaultValue() );
    String WEBSERVER_PORT_PROPERTY_KEY = ServerSettings.webserver_port.name();
    String DEFAULT_WEBSERVER_ADDRESS = ServerSettings.webserver_address.getDefaultValue();
    String WEBSERVER_ADDRESS_PROPERTY_KEY = ServerSettings.webserver_address.name();
    String WEBSERVER_MAX_THREADS_PROPERTY_KEY = ServerSettings.webserver_max_threads.name();
    String WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY = ServerSettings.webserver_limit_execution_time.name();
    String WEBSERVER_ENABLE_STATISTICS_COLLECTION = ServerInternalSettings.webserver_statistics_collection_enabled.name();

    String REST_API_PACKAGE = "org.neo4j.server.rest.web";
    String REST_API_PATH_PROPERTY_KEY = ServerInternalSettings.rest_api_path.name();
    String DEFAULT_DATA_API_PATH = ServerInternalSettings.rest_api_path.getDefaultValue();

    String DISCOVERY_API_PACKAGE = "org.neo4j.server.rest.discovery";

    String MANAGEMENT_API_PACKAGE = "org.neo4j.server.webadmin.rest";
    String MANAGEMENT_PATH_PROPERTY_KEY = ServerInternalSettings.management_api_path.name();
    String DEFAULT_MANAGEMENT_API_PATH = ServerInternalSettings.management_api_path.getDefaultValue();

    String BROWSER_PATH = ServerInternalSettings.browser_path.getDefaultValue();

    String RRDB_LOCATION_PROPERTY_KEY = ServerSettings.rrdb_location.name();

    String MANAGEMENT_CONSOLE_ENGINES = ServerSettings.management_console_engines.name();
    List<String> DEFAULT_MANAGEMENT_CONSOLE_ENGINES = new ArrayList<String>(){
        private static final long serialVersionUID = 6621747998288594121L;
    {
        add( ServerSettings.management_console_engines.getDefaultValue() );
    }};

    String THIRD_PARTY_PACKAGES_KEY = ServerSettings.third_party_packages.name();

    String SCRIPT_SANDBOXING_ENABLED_KEY = ServerInternalSettings.script_sandboxing_enabled.name();
    Boolean DEFAULT_SCRIPT_SANDBOXING_ENABLED = Boolean.valueOf( ServerInternalSettings.script_sandboxing_enabled.getDefaultValue() );

    String WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY = ServerSettings.webserver_https_enabled.name();
    Boolean DEFAULT_WEBSERVER_HTTPS_ENABLED = Boolean.valueOf( ServerSettings.webserver_https_enabled.getDefaultValue() );

    String WEBSERVER_HTTPS_PORT_PROPERTY_KEY = ServerSettings.webserver_https_port.name();
    int DEFAULT_WEBSERVER_HTTPS_PORT = Integer.valueOf( ServerSettings.webserver_https_port.getDefaultValue() );

    String WEBSERVER_KEYSTORE_PATH_PROPERTY_KEY = "org.neo4j.server.webserver.https.keystore.location";
    String DEFAULT_WEBSERVER_KEYSTORE_PATH = "";

    String WEBSERVER_HTTPS_CERT_PATH_PROPERTY_KEY = ServerSettings.tls_certificate_file.name();
    String DEFAULT_WEBSERVER_HTTPS_CERT_PATH = ServerSettings.tls_certificate_file.getDefaultValue();

    String WEBSERVER_HTTPS_KEY_PATH_PROPERTY_KEY = ServerSettings.tls_key_file.name();
    String DEFAULT_WEBSERVER_HTTPS_KEY_PATH = ServerSettings.tls_key_file.getDefaultValue();

    String HTTP_LOGGING = ServerSettings.http_logging_enabled.name();
    boolean DEFAULT_HTTP_LOGGING = Boolean.valueOf( ServerSettings.http_logging_enabled.getDefaultValue() );
    String HTTP_LOG_CONFIG_LOCATION = ServerSettings.http_log_config_file.name();

    String HTTP_CONTENT_LOGGING = ServerSettings.http_content_logging_enabled.name();
    boolean DEFAULT_HTTP_CONTENT_LOGGING = Boolean.valueOf( ServerSettings.http_content_logging_enabled.getDefaultValue() );

    String WADL_ENABLED = ServerInternalSettings.wadl_enabled.name();

    String STARTUP_TIMEOUT = ServerInternalSettings.startup_timeout.name();
    int DEFAULT_STARTUP_TIMEOUT = ( int ) ( TimeUtil.parseTimeMillis.apply( ServerInternalSettings.startup_timeout
            .getDefaultValue() ) / 1000 );
    String TRANSACTION_TIMEOUT = ServerSettings.transaction_timeout.name();
    int DEFAULT_TRANSACTION_TIMEOUT = ( int ) ( TimeUtil.parseTimeMillis.apply( ServerSettings.transaction_timeout
            .getDefaultValue() ) / 1000 );/*seconds*/;

    Configuration configuration();

    Map<String,String> getDatabaseTuningProperties();

    @Deprecated
    List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses();

    List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages();

    DiagnosticsExtractor<Configurator> DIAGNOSTICS = new DiagnosticsExtractor<Configurator>()
    {
        @Override
        public void dumpDiagnostics( final Configurator source, DiagnosticsPhase phase, Logger logger )
        {
            if ( phase.isInitialization() || phase.isExplicitlyRequested() )
            {
                final Configuration config = source.configuration();
                logger.log( "Server configuration:" );
                Iterator<String> keys = config.getKeys();
                while ( keys.hasNext() )
                {
                    String key = keys.next();
                    logger.log( "%s=%s", key, config.getProperty( key ) );
                }
            }
        }

        @Override
        public String toString()
        {
            return Configurator.class.getName();
        }
    };

    public static abstract class Adapter implements Configurator
    {
        @Override
        public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses()
        {
            return getThirdpartyJaxRsPackages();
        }

        @Override
        public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages()
        {
            return emptyList();
        }

        @Override
        public Map<String,String> getDatabaseTuningProperties()
        {
            return emptyMap();
        }

        @Override
        public Configuration configuration()
        {
            return new MapConfiguration( Collections.<String, String> emptyMap() );
        }
    }

    public static final Configurator EMPTY = new Configurator.Adapter()
    {
    };
}
