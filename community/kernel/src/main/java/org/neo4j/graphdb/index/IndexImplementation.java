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
package org.neo4j.graphdb.index;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A provider which can create and instantiate {@link Index}s.
 * An {@link IndexImplementation} is typically tied to one implementation, f.ex.
 * lucene, http://lucene.apache.org/java.
 *
 * @author Mattias Persson
 *
 */
public interface IndexImplementation
{
    /**
     * Returns the name of the XA data source coupled with this index provider.
     * @return the name of the XA data source coupled with this index provider.
     */
    String getDataSourceName();

    /**
     * Returns an {@link Index} for {@link Node}s for the name
     * {@code indexName} with the given {@code config}. The {@code config}
     * {@link Map} can contain any provider-implementation-specific data that
     * can control how an index behaves.
     *
     * @param indexName the name of the index.
     * @param config a {@link Map} of configuration parameters to use with the
     * index. Parameters can be anything and are implementation-specific. This
     * map represents how the configuration looks right now, they might be modified
     * later using {@link IndexManager#setConfiguration(Index, String, String)}
     * or {@link IndexManager#removeConfiguration(Index, String)}.
     * @return the {@link Index} corresponding to the {@code indexName} and
     * {@code config}.
     */
    Index<Node> nodeIndex( String indexName, Map<String, String> config );

    /**
     * Returns an {@link Index} for {@link Relationship}s for the name
     * {@code indexName} with the given {@code config}. The {@code config}
     * {@link Map} can contain any provider-implementation-specific data that
     * can control how an index behaves.
     *
     * @param indexName the name of the index.
     * @param config a {@link Map} of configuration parameters to use with the
     * index. Parameters can be anything and are implementation-specific. This
     * map represents how the configuration looks right now, they might be modified
     * later using {@link IndexManager#setConfiguration(Index, String, String)}
     * or {@link IndexManager#removeConfiguration(Index, String)}.
     * @return the {@link Index} corresponding to the {@code indexName} and
     * {@code config}. The return index is a {@link RelationshipIndex} with
     * additional query methods for efficiently filtering hits with respect to
     * start/end node of the relationships.
     */
    RelationshipIndex relationshipIndex( String indexName,
            Map<String, String> config );

    /**
     * Fills in default configuration parameters for indexes provided from this
     * index provider.
     * @param config the configuration map to complete with defaults.
     * @return a {@link Map} filled with decent defaults for an index from
     * this index provider.
     */
    Map<String, String> fillInDefaults( Map<String, String> config );

    boolean configMatches( Map<String, String> storedConfig, Map<String, String> config );
}
