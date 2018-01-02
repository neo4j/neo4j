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
package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

public class NodeIndexRootRepresentation extends MappingRepresentation
{
    private IndexManager indexManager;

    public NodeIndexRootRepresentation( IndexManager indexManager )
    {
        super( "node-index" );
        this.indexManager = indexManager;
    }

    @Override
    protected void serialize( final MappingSerializer serializer )
    {
        indexManager.nodeIndexNames();

        for ( String indexName : indexManager.nodeIndexNames() )
        {
            Index<Node> index = indexManager.forNodes( indexName );
            serializer.putMapping( indexName,
                    new NodeIndexRepresentation( indexName, indexManager.getConfiguration( index ) ) );
        }
    }
}
