/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.internal;

import java.net.URL;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.impl.store.StoreId;

/**
 * This API can be used to get access to services.
 */
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

    String getStoreDir();

    /**
     * Returns all relationship types currently in the underlying store.
     * Relationship types are added to the underlying store the first time they
     * are used in a successfully committed {@link Node#createRelationshipTo
     * node.createRelationshipTo(...)}. Note that this method is guaranteed to
     * return all known relationship types, but it does not guarantee that it
     * won't return <i>more</i> than that (e.g. it can return "historic"
     * relationship types that no longer have any relationships in the node
     * space).
     *
     * @return all relationship types in the underlying store
     */
    ResourceIterable<RelationshipType> getAllExistingRelationshipTypes();

    /**
     * Returns all labels currently in the underlying store. Labels are added to the store the first time
     * they are used. This method guarantees that it will return all labels currently in use. However,
     * it may also return <i>more</i> than that (e.g. it can return "historic" labels that are no longer used).
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all labels in the underlying store.
     */
    ResourceIterable<Label> getAllExistingLabelsInUseAndNot();

    /**
     * Returns all property keys currently in the underlying store. This method guarantees that it will return all
     * property keys currently in use. However, it may also return <i>more</i> than that (e.g. it can return "historic"
     * labels that are no longer used).
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all property keys in the underlying store.
     */
    ResourceIterable<String> getAllExistingPropertyKeysInUseAndNot();
}
