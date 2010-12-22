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

package org.neo4j.server.configuration.validation;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.configuration.Configurator;

public class WebadminConfigurationRule implements ValidationRule {
    @Override
    public void validate(Configuration configuration) throws RuleFailedException {
        String managementApi = validateConfigurationContainsKey(configuration, Configurator.WEB_ADMIN_PATH_PROPERTY_KEY);
        String restApi = validateConfigurationContainsKey(configuration, Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY);

        // Check URIs are ok
        URI managementUri =  validateUri(managementApi, Configurator.WEB_ADMIN_PATH_PROPERTY_KEY);
        URI restUri = validateUri(restApi, Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY);

            
        // Is it relative? if so change to absolute
        managementUri = makeAbsoluteAndNormalized((String)configuration.getProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY), managementUri);
        restUri = makeAbsoluteAndNormalized((String)configuration.getProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY), restUri);
        
        // Overwrite the properties with the new normalised, absolute URIs
        configuration.clearProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY);
        configuration.addProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY, managementUri.toString());
        
        configuration.clearProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY);
        configuration.addProperty(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY, restUri.toString());
    }
    
    private URI makeAbsoluteAndNormalized(String portNo, URI uri) {
        if(uri.isAbsolute()) return uri.normalize();
        
        if(portNo == null) portNo = "80";
        
        StringBuilder sb = new StringBuilder();
        sb.append("http://localhost");
        if(portNo != "80") {
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

    private URI validateUri(String uri, String property) {
        URI result = null;
        try {
            result = new URI(uri);
        } catch (URISyntaxException e) {
            new RuleFailedException("The specified URI [%s] for the property [%s] is invalid. Please correct the neo4j-server.properties file.", uri ,property);
        }
        return result;
    }

    private String validateConfigurationContainsKey(Configuration configuration, String key) throws RuleFailedException {
        if(!configuration.containsKey(key)) {
            throw new RuleFailedException("Webadmin configuration not found. Check that the neo4j-server.properties file contains the [%s] property.", key);
        }
        
        return (String)configuration.getProperty(key);
    }
}
