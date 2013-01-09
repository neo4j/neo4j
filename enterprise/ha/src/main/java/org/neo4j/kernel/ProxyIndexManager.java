/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;

public class ProxyIndexManager implements IndexManager
{
    private IndexManager indexManager;
    
    void setDelegate( IndexManager indexManager )
    {
        this.indexManager = indexManager;
    }

    public boolean existsForNodes( String indexName )
    {
        return indexManager.existsForNodes( indexName );
    }

    public Index<Node> forNodes( String indexName )
    {
        return indexManager.forNodes( indexName );
    }

    public Index<Node> forNodes( String indexName, Map<String, String> customConfiguration )
    {
        return indexManager.forNodes( indexName, customConfiguration );
    }

    public String[] nodeIndexNames()
    {
        return indexManager.nodeIndexNames();
    }

    public boolean existsForRelationships( String indexName )
    {
        return indexManager.existsForRelationships( indexName );
    }

    public RelationshipIndex forRelationships( String indexName )
    {
        return indexManager.forRelationships( indexName );
    }

    public RelationshipIndex forRelationships( String indexName, Map<String, String> customConfiguration )
    {
        return indexManager.forRelationships( indexName, customConfiguration );
    }

    public String[] relationshipIndexNames()
    {
        return indexManager.relationshipIndexNames();
    }

    public Map<String, String> getConfiguration( Index<? extends PropertyContainer> index )
    {
        return indexManager.getConfiguration( index );
    }

    public String setConfiguration( Index<? extends PropertyContainer> index, String key, String value )
    {
        return indexManager.setConfiguration( index, key, value );
    }

    public String removeConfiguration( Index<? extends PropertyContainer> index, String key )
    {
        return indexManager.removeConfiguration( index, key );
    }

    public AutoIndexer<Node> getNodeAutoIndexer()
    {
        return indexManager.getNodeAutoIndexer();
    }

    public RelationshipAutoIndexer getRelationshipAutoIndexer()
    {
        return indexManager.getRelationshipAutoIndexer();
    }
}
