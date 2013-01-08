/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.tooling;

import java.util.Iterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;

/**
 * A tool for doing global operations, for example {@link #getAllNodes()}.
 */
public class GlobalGraphOperations
{
    private final NodeManager nodeManager;

    private GlobalGraphOperations( GraphDatabaseService db )
    {
        this.nodeManager = ((GraphDatabaseAPI) db).getNodeManager();
    }

    /**
     * Get a {@link GlobalGraphOperations} for the given {@code db}.
     * 
     * @param db
     *            the {@link GraphDatabaseService} to get global operations for.
     * @return a {@link GlobalGraphOperations} for the given {@code db}.
     */
    public static GlobalGraphOperations at( GraphDatabaseService db )
    {
        return new GlobalGraphOperations( db );
    }

    /**
     * Returns all nodes in the graph.
     * 
     * @return all nodes in the graph.
     */
    public Iterable<Node> getAllNodes()
    {
        return new Iterable<Node>()
        {
            @Override
            public Iterator<Node> iterator()
            {
                return nodeManager.getAllNodes();
            }
        };
    }

    /**
     * Returns all relationships in the graph.
     * 
     * @return all relationships in the graph.
     */
    public Iterable<Relationship> getAllRelationships()
    {
        return new Iterable<Relationship>()
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                return nodeManager.getAllRelationships();
            }
        };
    }

    /**
     * Returns all relationship types currently in the underlying store. Relationship types are
     * added to the underlying store the first time they are used in a successfully commited
     * {@link Node#createRelationshipTo node.createRelationshipTo(...)}. Note that this method is
     * guaranteed to return all known relationship types, but it does not guarantee that it won't
     * return <i>more</i> than that (e.g. it can return "historic" relationship types that no longer
     * have any relationships in the node space).
     * 
     * @return all relationship types in the underlying store
     */
    public Iterable<RelationshipType> getAllRelationshipTypes()
    {
        return nodeManager.getRelationshipTypes();
    }
}
