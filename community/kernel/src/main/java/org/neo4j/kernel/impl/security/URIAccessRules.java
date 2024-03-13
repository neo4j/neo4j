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

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Set;
import org.neo4j.configuration.Config;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.exceptions.LoadExternalResourceException;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;

public class URIAccessRules {

    private static final Set<String> WEB_SCHEMES = Set.of("http", "https", "ftp");

    private final SecurityAuthorizationHandler securityAuthorizationHandler;
    private final WebURLAccessRule webAccess;
    private final FileURIAccessRule fileAccess;

    public URIAccessRules(AbstractSecurityLog securityLog, Config configuration) {
        this(securityLog, new WebURLAccessRule(configuration), new FileURIAccessRule(configuration));
    }

    public URIAccessRules(AbstractSecurityLog securityLog, WebURLAccessRule webAccess, FileURIAccessRule fileAccess) {
        this.securityAuthorizationHandler = new SecurityAuthorizationHandler(securityLog);
        this.webAccess = webAccess;
        this.fileAccess = fileAccess;
    }

    public WebURLAccessRule webAccess() {
        return this.webAccess;
    }

    public CharReadable validateAndOpen(SecurityContext securityContext, URI uri) throws URLAccessValidationError {
        String scheme = uri.getScheme();
        if (scheme == null) {
            // even though we're working with URIs - report as URL errors for compat with original error messages
            throw new LoadExternalResourceException(String.format("Invalid URL '%s': no scheme", uri));
        }
        try {
            if (WEB_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))) {
                return webAccess.getReader(uri.toURL(), securityAuthorizationHandler, securityContext);
            } else {
                return fileAccess.getReader(uri, securityAuthorizationHandler, securityContext);
            }
        } catch (IOException e) {
            throw new LoadExternalResourceException(
                    String.format("Couldn't load the external resource at: %s", uri), e);
        }
    }
}
