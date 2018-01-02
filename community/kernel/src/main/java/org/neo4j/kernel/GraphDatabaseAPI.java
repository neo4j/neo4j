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
package org.neo4j.kernel;

import java.net.URL;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.security.URLAccessValidationError;

/**
 * This API can be used to get access to services.
 *
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public interface GraphDatabaseAPI extends GraphDatabaseService
{
    /**
     * Look up database components for direct access.
     * Usage of this method is generally an indication of architectural error.
     */
    DependencyResolver getDependencyResolver();

    /** Provides the unique id assigned to this database. */
    StoreId storeId();

    /**
     * Validate whether this database instance is permitted to reach out to the specified URL (e.g. when using {@code LOAD CSV} in Cypher).
     *
     * @param url the URL being validated
     * @return an updated URL that should be used for accessing the resource
     */
    URL validateURLAccess( URL url ) throws URLAccessValidationError;

    @Deprecated
    String getStoreDir();
}
