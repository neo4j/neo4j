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
package org.neo4j.kernel.impl.security;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;

public class URLAccessRules {

    private final SecurityAuthorizationHandler securityAuthorizationHandler;
    private final Map<String, URLAccessRule> rules;

    private final WebURLAccessRule webAccess;

    public URLAccessRules(AbstractSecurityLog securityLog, Configuration configuration) {
        this.securityAuthorizationHandler = new SecurityAuthorizationHandler(securityLog);
        this.webAccess = new WebURLAccessRule(configuration);
        rules = new HashMap<>();
        rules.put("http", webAccess);
        rules.put("https", webAccess);
        rules.put("ftp", webAccess);
        rules.put("file", FILE_ACCESS);
    }

    private static final FileURLAccessRule FILE_ACCESS = new FileURLAccessRule();

    public static URLAccessRule fileAccess() {
        return FILE_ACCESS;
    }

    public WebURLAccessRule webAccess() {
        return this.webAccess;
    }

    public URL validate(Configuration configuration, SecurityContext securityContext, URL url)
            throws URLAccessValidationError {
        securityAuthorizationHandler.assertLoadAllowed(securityContext, url);
        String protocol = url.getProtocol();
        URLAccessRule protocolRule = rules.get(protocol);
        if (protocolRule == null) {
            throw new URLAccessValidationError("loading resources via protocol '" + protocol + "' is not permitted");
        }
        return protocolRule.validate(configuration, url);
    }
}
