/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import java.util.Iterator;

public class StatementContext
{
    private final Statement statement;
    private final GraphDatabaseService graph;
    private final RegisterFactory registerFactory;

    public StatementContext( Statement statement, GraphDatabaseService graph, RegisterFactory registerFactory )
    {
        this.statement = statement;
        this.graph = graph;
        this.registerFactory = registerFactory;
    }

    public StatementContext( Statement statement, GraphDatabaseService graph )
    {
        this( statement, graph, ArrayRegisters.FACTORY );
    }

    public ReadOperations read()
    {
        return statement.readOperations();
    }


    public RegisterFactory registerFactory()
    {
        return registerFactory;

    }
    public PrimitiveLongIterator FAKEgetAllNodes()
    {
        final Iterator<Node> allNodes = graph.getAllNodes().iterator();
        return new PrimitiveLongIterator()
        {
            @Override
            public boolean hasNext()
            {
                return allNodes.hasNext();
            }

            @Override
            public long next()
            {
                return allNodes.next().getId();
            }
        };
    }

    public PrimitiveLongIterator FAKEgetNodesRelatedBy( long nodeId, Direction direction )
    {
        final Node nodeById = graph.getNodeById(nodeId);
        final Iterator<Relationship> relationships = nodeById.getRelationships(direction).iterator();
        return new PrimitiveLongIterator()
        {
            @Override
            public boolean hasNext()
            {
                return relationships.hasNext();
            }

            @Override
            public long next()
            {
                return relationships.next().getOtherNode(nodeById).getId();
            }
        };
    }
}
