/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.server.webadmin.console.ShellSessionCreator;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public interface Configurator
{
    String SECURITY_RULES_KEY = "org.neo4j.server.rest.security_rules";

    String DB_TUNING_PROPERTY_FILE_KEY = "org.neo4j.server.db.tuning.properties";
    String DEFAULT_CONFIG_DIR = File.separator + "etc" + File.separator + "neo";
    String DATABASE_LOCATION_PROPERTY_KEY = "org.neo4j.server.database.location";
    String NEO_SERVER_CONFIG_FILE_KEY = "org.neo4j.server.properties";
    String DB_MODE_KEY = "org.neo4j.server.database.mode";

    String DEFAULT_DATABASE_LOCATION_PROPERTY_KEY = "data/graph.db";

    int DEFAULT_WEBSERVER_PORT = 7474;
    String WEBSERVER_PORT_PROPERTY_KEY = "org.neo4j.server.webserver.port";
    String DEFAULT_WEBSERVER_ADDRESS = "localhost";
    String WEBSERVER_ADDRESS_PROPERTY_KEY = "org.neo4j.server.webserver.address";
    String WEBSERVER_MAX_THREADS_PROPERTY_KEY = "org.neo4j.server.webserver.maxthreads";
    String WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY = "org.neo4j.server.webserver.limit.executiontime";
    String WEBSERVER_ENABLE_STATISTICS_COLLECTION = "org.neo4j.server.webserver.statistics";

    String REST_API_PATH_PROPERTY_KEY = "org.neo4j.server.webadmin.data.uri";
    String REST_API_PACKAGE = "org.neo4j.server.rest.web";
    String DEFAULT_DATA_API_PATH = "/db/data";

    String DISCOVERY_API_PACKAGE = "org.neo4j.server.rest.discovery";

    String MANAGEMENT_API_PACKAGE = "org.neo4j.server.webadmin.rest";
    String MANAGEMENT_PATH_PROPERTY_KEY = "org.neo4j.server.webadmin.management.uri";
    String DEFAULT_MANAGEMENT_API_PATH = "/db/manage";

    String BROWSER_PATH = "/browser";

    String RRDB_LOCATION_PROPERTY_KEY = "org.neo4j.server.webadmin.rrdb.location";

    String MANAGEMENT_CONSOLE_ENGINES = "org.neo4j.server.manage.console_engines";
    List<String> DEFAULT_MANAGEMENT_CONSOLE_ENGINES = new ArrayList<String>(){
        private static final long serialVersionUID = 6621747998288594121L;
    {
        add(new ShellSessionCreator().name());
    }};

    String THIRD_PARTY_PACKAGES_KEY = "org.neo4j.server.thirdparty_jaxrs_classes";

    String SCRIPT_SANDBOXING_ENABLED_KEY = "org.neo4j.server.script.sandboxing.enabled";
    Boolean DEFAULT_SCRIPT_SANDBOXING_ENABLED = true;

    String WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY = "org.neo4j.server.webserver.https.enabled";
    Boolean DEFAULT_WEBSERVER_HTTPS_ENABLED = false;

    String WEBSERVER_HTTPS_PORT_PROPERTY_KEY = "org.neo4j.server.webserver.https.port";
    int DEFAULT_WEBSERVER_HTTPS_PORT = 7473;

    String WEBSERVER_KEYSTORE_PATH_PROPERTY_KEY = "org.neo4j.server.webserver.https.keystore.location";
    String DEFAULT_WEBSERVER_KEYSTORE_PATH = "neo4j-home/ssl/keystore";

    String WEBSERVER_HTTPS_CERT_PATH_PROPERTY_KEY = "org.neo4j.server.webserver.https.cert.location";
    String DEFAULT_WEBSERVER_HTTPS_CERT_PATH = "neo4j-home/ssl/snakeoil.cert";

    String WEBSERVER_HTTPS_KEY_PATH_PROPERTY_KEY = "org.neo4j.server.webserver.https.key.location";
    String DEFAULT_WEBSERVER_HTTPS_KEY_PATH = "neo4j-home/ssl/snakeoil.key";

    String HTTP_LOGGING = "org.neo4j.server.http.log.enabled";
    boolean DEFAULT_HTTP_LOGGING = false;
    String HTTP_LOG_CONFIG_LOCATION = "org.neo4j.server.http.log.config";
    String WADL_ENABLED = "unsupported_wadl_generation_enabled";

    String STARTUP_TIMEOUT = "org.neo4j.server.startup_timeout";
    int DEFAULT_STARTUP_TIMEOUT = 120;

    String TRANSACTION_TIMEOUT = "org.neo4j.server.transaction.timeout";
    int DEFAULT_TRANSACTION_TIMEOUT = 60/*seconds*/;

    Configuration configuration();

    Map<String, String> getDatabaseTuningProperties();

    @Deprecated
    List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses();

    List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages();

    DiagnosticsExtractor<Configurator> DIAGNOSTICS = new DiagnosticsExtractor<Configurator>()
    {
        @Override
        public void dumpDiagnostics( final Configurator source, DiagnosticsPhase phase, StringLogger log )
        {
            if ( phase.isInitialization() || phase.isExplicitlyRequested() )
            {
                final Configuration config = source.configuration();
                log.logLongMessage( "Server configuration:", new PrefetchingIterator<String>()
                {
                    final Iterator<?> keys = config.getKeys();

                    @Override
                    protected String fetchNextOrNull()
                    {
                        while ( keys.hasNext() )
                        {
                            Object key = keys.next();
                            if ( key instanceof String )
                            {
                                return key + " = " + config.getProperty( (String) key );
                            }
                        }
                        return null;
                    }
                }, true );
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
        public Map<String, String> getDatabaseTuningProperties()
        {
            return emptyMap();
        }

        @Override
        public Configuration configuration()
        {
            return new MapConfiguration( emptyMap() );
        }
    }

    public static final Configurator EMPTY = new Configurator.Adapter()
    {
    };
}
