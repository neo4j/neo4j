/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

/**
 * Holds statistics for the execution of a query.
 */
public class QueryStatistics
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
    public int getNodesCreated()
    {
        return inner.nodesCreated();
    }

    /**
     * Returns the number of relationships created by this query.
     * @return the number of relationships created by this query.
     */
    public int getRelationshipsCreated()
    {
        return inner.relationshipsCreated();
    }

    /**
     * Returns the number of properties set by this query. Setting a property to the same value again still counts towards this.
     * @return the number of properties set by this query.
     */
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
    public int getLabelsAdded()
    {
        return inner.labelsAdded();
    }

    /**
     * Returns the number of labels removed from any node by this query.
     * @return the number of labels removed from any node by this query.
     */
    public int getLabelsRemoved()
    {
        return inner.labelsRemoved();
    }

    /**
     * Returns the number of indexes added by this query.
     * @return the number of indexes added by this query.
     */
    public int getIndexesAdded()
    {
        return inner.indexesAdded();
    }

    /**
     * Returns the number of indexes removed by this query.
     * @return the number of indexes removed by this query.
     */
    public int getIndexesRemoved()
    {
        return inner.indexesRemoved();
    }

    /**
     * Returns the number of constraint added by this query.
     * @return the number of constraint added by this query.
     */
    public int getConstraintsAdded()
    {
        return inner.constraintsAdded();
    }

    /**
     * Returns the number of constraint removed by this query.
     * @return the number of constraint removed by this query.
     */
    public int getConstraintsRemoved()
    {
        return inner.constraintsRemoved();
    }

    /**
     * If the query updated the graph in any way, this method will return true.
     * @return if the graph has been updated.
     */
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
