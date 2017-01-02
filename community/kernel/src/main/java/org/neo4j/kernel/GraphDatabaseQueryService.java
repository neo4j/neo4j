/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

/*
 * This is a trimmed down version of GraphDatabaseService and GraphDatabaseAPI, limited to a subset of functions needed
 * by implementations of QueryExecutionEngine.
 */
public interface GraphDatabaseQueryService
{
    DependencyResolver getDependencyResolver();

    Node createNode();

    Node createNode( Label... labels );

    Node getNodeById( long id );

    Relationship getRelationshipById( long id );

    /**
     * Begin new internal transaction with with default timeout.
     *
     * @param type transaction type
     * @param securityContext transaction security context
     * @return internal transaction
     */
    InternalTransaction beginTransaction( KernelTransaction.Type type, SecurityContext securityContext );

    /**
     * Begin new internal transaction with specified timeout in milliseconds.
     *
     * @param type transaction type
     * @param securityContext transaction security context
     * @param timeout transaction timeout
     * @param unit time unit of timeout argument
     * @return internal transaction
     */
    InternalTransaction beginTransaction( KernelTransaction.Type type, SecurityContext securityContext, long timeout,
            TimeUnit unit);

    URL validateURLAccess( URL url ) throws URLAccessValidationError;

    DbmsOperations getDbmsOperations();
}
