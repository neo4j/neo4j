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
import java.net.URL;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.exceptions.LoadExternalResourceException;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;

public class URLAccessRules {

    private final SecurityAuthorizationHandler securityAuthorizationHandler;
    private final WebURLAccessRule webAccess;
    private final FileURLAccessRule fileAccess;

    public URLAccessRules(AbstractSecurityLog securityLog, Configuration configuration) {
        this.securityAuthorizationHandler = new SecurityAuthorizationHandler(securityLog);
        this.webAccess = new WebURLAccessRule(configuration);
        this.fileAccess = new FileURLAccessRule(configuration);
    }

    public WebURLAccessRule webAccess() {
        return this.webAccess;
    }

    public CharReadable validateAndOpen(SecurityContext securityContext, URL url) throws URLAccessValidationError {

        String protocol = url.getProtocol();
        try {
            return switch (protocol) {
                case "file" -> fileAccess.getReader(url, securityAuthorizationHandler, securityContext);
                case "http", "https", "ftp" -> webAccess.getReader(url, securityAuthorizationHandler, securityContext);
                default -> throw new URLAccessValidationError(
                        "loading resources via protocol '" + protocol + "' is not permitted");
            };
        } catch (IOException e) {
            throw new LoadExternalResourceException(
                    String.format("Couldn't load the external resource at: %s", url), e);
        }
    }
}
