/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.ssl;

import static org.eclipse.jetty.http.HttpHeader.STRICT_TRANSPORT_SECURITY;
import static org.neo4j.server.configuration.ServerSettings.http_strict_transport_security;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.PreEncodedHttpField;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.neo4j.configuration.Config;

public class HttpsRequestCustomizer implements HttpConfiguration.Customizer {
    private final HttpField hstsResponseField;

    public HttpsRequestCustomizer(Config config) {
        hstsResponseField = createHstsResponseField(config);
    }

    @Override
    public void customize(Connector connector, HttpConfiguration channelConfig, Request request) {
        request.setHttpURI(HttpURI.build(request.getHttpURI()).scheme(HttpScheme.HTTPS));
        addResponseFieldIfConfigured(request, hstsResponseField);
    }

    private static void addResponseFieldIfConfigured(Request request, HttpField field) {
        if (field != null) {
            request.getResponse().getHttpFields().add(field);
        }
    }

    private static HttpField createHstsResponseField(Config config) {
        String configuredValue = config.get(http_strict_transport_security);
        if (StringUtils.isBlank(configuredValue)) {
            return null;
        }
        return new PreEncodedHttpField(STRICT_TRANSPORT_SECURITY, configuredValue);
    }
}
