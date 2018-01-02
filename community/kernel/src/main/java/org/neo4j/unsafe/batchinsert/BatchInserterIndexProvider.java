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
package org.neo4j.unsafe.batchinsert;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A place to access {@link BatchInserterIndex}s from a certain index provider.
 * Use together with {@link BatchInserter} to create indexes which later can be
 * accessed through {@link GraphDatabaseService#index()}.
 * 
 * @author Mattias Persson
 *
 */
public interface BatchInserterIndexProvider
{
    /**
     * Returns a {@link BatchInserterIndex} for {@link Node}s for the name
     * {@code indexName} with the given {@code config}. The {@code config}
     * {@link Map} can contain any provider-implementation-specific data that
     * can control how an index behaves.
     * 
     * @param indexName the name of the index. It will be created if it doesn't
     *            exist.
     * @param config a {@link Map} of configuration parameters to use with the
     *            index if it doesn't exist. Parameters can be anything and are
     *            implementation-specific.
     * @return the {@link BatchInserterIndex} corresponding to the
     *         {@code indexName}.
     */
    BatchInserterIndex nodeIndex( String indexName, Map<String, String> config );
    
    /**
     * Returns a {@link BatchInserterIndex} for {@link Relationship}s for the
     * name {@code indexName} with the given {@code config}. The {@code config}
     * {@link Map} can contain any provider-implementation-specific data that
     * can control how an index behaves.
     * 
     * @param indexName the name of the index. It will be created if it doesn't
     *            exist.
     * @param config a {@link Map} of configuration parameters to use with the
     *            index if it doesn't exist. Parameters can be anything and are
     *            implementation-specific.
     * @return the {@link BatchInserterIndex} corresponding to the
     *         {@code indexName}.
     */
    BatchInserterIndex relationshipIndex( String indexName, Map<String, String> config );

    /**
     * Shuts down this index provider and ensures that all indexes are fully
     * written to disk. If this method isn't called before shutting down the
     * {@link BatchInserter} there's no guaranteed that data added to indexes
     * will be persisted.
     */
    void shutdown();
}
