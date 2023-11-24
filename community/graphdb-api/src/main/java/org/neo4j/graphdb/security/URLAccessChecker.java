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
package org.neo4j.graphdb.security;

import java.net.URL;

public interface URLAccessChecker {
    /**
     * Check a URL is compliant with security rules running in the database. This will be a combination of RBAC rules and static
     * configuration. This is primarily intended to decouple APOC from using internal things directly to do this.
     * @param url the URL being validated
     * @return an updated URL that should be used for accessing the resource
     * @throws URLAccessValidationError thrown if the url does not pass the validation rule
     */
    public URL checkURL(URL url) throws URLAccessValidationError;
}
