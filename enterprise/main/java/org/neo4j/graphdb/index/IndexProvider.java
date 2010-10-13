/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.graphdb.index;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Service;

import java.util.Map;

/**
 * A provider which can create and instantiate {@link Index}s.
 * An {@link IndexProvider} is typically tied to one type of index, f.ex.
 * lucene, http://lucene.apache.org/java.
 * 
 * @author Mattias Persson
 *
 */
public abstract class IndexProvider extends Service
{
    protected IndexProvider( String key, String... altKeys )
    {
        super( key, altKeys );
    }

    /**
     * Returns an {@link Index} for {@link Node}s for the name
     * {@code indexName} with the given {@code config}. The {@code config}
     * {@link Map} can contain any provider-implementation-specific data that
     * can control how an index behaves.
     * 
     * @param indexName the for the database unique name of the index. Node indexes and
     * relationship indexes do not share the same name space, and can have the same name.
     * @param config a {@link Map} of configuration parameters to use with the
     * index. Parameters can be anything and are implementation-specific.
     * @return the {@link Index} corresponding to the {@code indexName} and
     * {@code config}.
     */
    public abstract Index<Node> nodeIndex( String indexName, Map<String, String> config );
    
    /**
     * Returns an {@link Index} for {@link Relationship}s for the name
     * {@code indexName} with the given {@code config}. The {@code config}
     * {@link Map} can contain any provider-implementation-specific data that
     * can control how an index behaves.
     * 
     * @param indexName the for the database unique name of the index. Node indexes and
     * relationship indexes do not share the same name space, and can have the same name. 
     * @param config a {@link Map} of configuration parameters to use with the
     * index. Parameters can be anything and are implementation-specific.
     * @return the {@link Index} corresponding to the {@code indexName} and
     * {@code config}. The return index is a {@link RelationshipIndex} with
     * additional query methods for efficiently filtering hits with respect to
     * start/end node of the relationships.
     */
    public abstract RelationshipIndex relationshipIndex( String indexName,
            Map<String, String> config );
}
