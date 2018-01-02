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
package org.neo4j.server.rest.security;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.server.configuration.Configurator;

public interface SecurityRule
{
    String DEFAULT_DATABASE_PATH = Configurator.DEFAULT_DATA_API_PATH;

    /**
     * @param request The HTTP request currently under consideration.
     * @return <code>true</code> if the rule passes, <code>false</code> if the
     *         rule fails and the request is to be rejected with a "401 Unauthorized".
     */
    boolean isAuthorized(HttpServletRequest request);

    /**
     * @return the root of the URI path from which rules will be valid, e.g.
     *         <code>/db/data</code> will apply this rule to everything below
     *         the path <code>/db/data</code> It is possible to use * as a
     *         wildcard character in return values, e.g.
     *         <code>/myExtension*</code> will extend security coverage to
     *         everything under the <code>/myExtension</code> path. Similarly
     *         more complex path behavior can be specified with more wildcards,
     *         e.g.: <code>/myExtension*myApplication*specialResources</code>.
     *         Note that the wildcard represents any character (including the
     *         '/' character), meaning <code>/myExtension/*</code> is not the
     *         same as <code>/myExtension*</code> and implementers should take
     *         care to ensure their implementations are tested accordingly.
     *         <p>
     *         Final note: the only wildcard supported is '*' and there is no
     *         support for regular expression syntax.
     */
    String forUriPath();

    /**
     * @return the opaque string representing the WWW-Authenticate header to
     *         which the rule applies. Will be used to formulate a
     *         <code>401</code> response code if the rule denies a request.
     */
    String wwwAuthenticateHeader();
}
