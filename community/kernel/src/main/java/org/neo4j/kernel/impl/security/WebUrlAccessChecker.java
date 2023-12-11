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
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;

public class WebUrlAccessChecker implements URLAccessChecker {

    private final WebURLAccessRule webURLAccessRule;
    private final SecurityAuthorizationHandler securityAuthorizationHandler;
    private final SecurityContext securityContext;

    public WebUrlAccessChecker(
            WebURLAccessRule webURLAccessRule,
            SecurityAuthorizationHandler securityAuthorizationHandler,
            SecurityContext securityContext) {
        this.webURLAccessRule = webURLAccessRule;
        this.securityAuthorizationHandler = securityAuthorizationHandler;
        this.securityContext = securityContext;
    }

    @Override
    public URL checkURL(URL url) throws URLAccessValidationError {
        try {
            return webURLAccessRule.checkNotBlockedAndPinToIP(url, securityAuthorizationHandler, securityContext);
        } catch (Exception e) {
            if (e instanceof URLAccessValidationError) {
                throw (URLAccessValidationError) e;
            } else {
                throw new URLAccessValidationError(
                        "Unable to verify access to " + url.getHost() + ". Cause: " + e.getMessage());
            }
        }
    }
}
