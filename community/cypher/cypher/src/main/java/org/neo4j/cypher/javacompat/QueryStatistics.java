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
package org.neo4j.cypher.javacompat;

import java.util.Map;

/**
 * Holds statistics for the execution of a query.
 *
 * @deprecated See {@link org.neo4j.graphdb.QueryStatistics} which you can get from {@link org.neo4j.graphdb.Result}
 * when using {@link org.neo4j.graphdb.GraphDatabaseService#execute(String, Map)}.
 */
@Deprecated
public class QueryStatistics implements org.neo4j.graphdb.QueryStatistics
{
    private final org.neo4j.cypher.QueryStatistics inner;

    QueryStatistics( org.neo4j.cypher.QueryStatistics inner )
    {
        this.inner = inner;
    }

    /**
     * Returns the number of nodes created by this query.
     * @return the number of nodes created by this query.
     */
    @Override
    public int getNodesCreated()
    {
        return inner.nodesCreated();
    }

    @Override
    public int getNodesDeleted()
    {
        return inner.nodesDeleted();
    }

    /**
     * Returns the number of relationships created by this query.
     * @return the number of relationships created by this query.
     */
    @Override
    public int getRelationshipsCreated()
    {
        return inner.relationshipsCreated();
    }

    @Override
    public int getRelationshipsDeleted()
    {
        return inner.relationshipsDeleted();
    }

    /**
     * Returns the number of properties set by this query. Setting a property to the same value again still counts towards this.
     * @return the number of properties set by this query.
     */
    @Override
    public int getPropertiesSet()
    {
        return inner.propertiesSet();
    }

    /**
     * Returns the number of nodes deleted by this query.
     * @return the number of nodes deleted by this query.
     */
    public int getDeletedNodes()
    {
        return inner.nodesDeleted();
    }

    /**
     * Returns the number of relationships deleted by this query.
     * @return the number of relationships deleted by this query.
     */
    public int getDeletedRelationships()
    {
        return inner.relationshipsDeleted();
    }

    /**
     * Returns the number of labels added to any node by this query.
     * @return the number of labels added to any node by this query.
     */
    @Override
    public int getLabelsAdded()
    {
        return inner.labelsAdded();
    }

    /**
     * Returns the number of labels removed from any node by this query.
     * @return the number of labels removed from any node by this query.
     */
    @Override
    public int getLabelsRemoved()
    {
        return inner.labelsRemoved();
    }

    /**
     * Returns the number of indexes added by this query.
     * @return the number of indexes added by this query.
     */
    @Override
    public int getIndexesAdded()
    {
        return inner.indexesAdded();
    }

    /**
     * Returns the number of indexes removed by this query.
     * @return the number of indexes removed by this query.
     */
    @Override
    public int getIndexesRemoved()
    {
        return inner.indexesRemoved();
    }

    /**
     * Returns the number of constraint added by this query.
     * @return the number of constraint added by this query.
     */
    @Override
    public int getConstraintsAdded()
    {
        return inner.constraintsAdded();
    }

    /**
     * Returns the number of constraint removed by this query.
     * @return the number of constraint removed by this query.
     */
    @Override
    public int getConstraintsRemoved()
    {
        return inner.constraintsRemoved();
    }

    /**
     * If the query updated the graph in any way, this method will return true.
     * @return if the graph has been updated.
     */
    @Override
    public boolean containsUpdates()
    {
        return inner.containsUpdates();
    }

    @Override
    public String toString()
    {
        return inner.toString();
    }
}
