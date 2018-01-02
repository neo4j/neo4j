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

/**
 * A variant of SecurityRule which adds the ability to forbid access, even if the request is authenticated,
 * which results in an HTTP 403.
 */
public interface ForbiddingSecurityRule extends SecurityRule {

    /**
     * @param request The HTTP request currently under consideration.
     * @return <code>true</code> if the rule is triggered and the request is to be rejected with a "403 Forbidden",
     *  and <code>false</code> otherwise.
     */
    boolean isForbidden(HttpServletRequest request);

}
