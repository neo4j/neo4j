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
package org.neo4j.graphdb.security;

import java.net.URL;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.security.URLAccessValidationError;

/**
 * A rule to evaluate if Neo4j is permitted to reach out to the specified URL (e.g. when using {@code LOAD CSV} in Cypher).
 */
public interface URLAccessRule
{
    /**
     * Validate this rule against the specified URL and configuration, and throw a {@link URLAccessValidationError}
     * if the URL is not permitted for access.
     *
     * @param gdb the current graph database
     * @param url the URL being validated
     * @return an updated URL that should be used for accessing the resource
     * @throws URLAccessValidationError thrown if the url does not pass the validation rule
     */
    URL validate( GraphDatabaseAPI gdb, URL url ) throws URLAccessValidationError;
}
