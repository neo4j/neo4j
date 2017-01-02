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
package org.neo4j.cypher.javacompat.internal;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class GraphDatabaseCypherService implements GraphDatabaseQueryService
{
    private final GraphDatabaseFacade graph;
    private final DbmsOperations dbmsOperations;

    public GraphDatabaseCypherService( GraphDatabaseService graph )
    {
        this.graph = (GraphDatabaseFacade) graph;
        this.dbmsOperations = getDependencyResolver().resolveDependency( DbmsOperations.class );
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return graph.getDependencyResolver();
    }

    @Override
    public Node createNode()
    {
        return graph.createNode();
    }

    @Override
    public Node createNode( Label... labels )
    {
        return graph.createNode( labels );
    }

    @Override
    public Node getNodeById( long id )
    {
        return graph.getNodeById( id );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return graph.getRelationshipById( id );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, SecurityContext securityContext )
    {
        return graph.beginTransaction( type, securityContext );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, SecurityContext securityContext,
            long timeout, TimeUnit unit )
    {
        return graph.beginTransaction( type, securityContext, timeout, unit );
    }

    @Override
    public URL validateURLAccess( URL url ) throws URLAccessValidationError
    {
        return graph.validateURLAccess( url );
    }

    @Override
    public DbmsOperations getDbmsOperations() {
        return dbmsOperations;
    }

    // This provides backwards compatibility to the older API for places that cannot (yet) stop using it.
    // TODO: Remove this when possible (remove RULE, remove older compilers)
    public GraphDatabaseFacade getGraphDatabaseService()
    {
        return graph;
    }
}
