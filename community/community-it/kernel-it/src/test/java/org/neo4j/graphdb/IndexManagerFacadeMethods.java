/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphdb;

import java.util.function.Consumer;

import org.neo4j.graphdb.index.IndexManager;

public enum IndexManagerFacadeMethods implements Consumer<IndexManager>
{
    EXISTS_FOR_NODES( new FacadeMethod<>( "boolean existsForNodes( String indexName )", self -> self.existsForNodes( "foo" ) ) ),
    FOR_NODES( new FacadeMethod<>( "Index<Node> forNodes( String indexName )", self -> self.forNodes( "foo" ) ) ),
    FOR_NODES_WITH_CONFIG(
            new FacadeMethod<>( "Index<Node> forNodes( String indexName, Map<String, String> customConfiguration )", self -> self.forNodes( "foo", null ) ) ),
    NODE_INDEX_NAMES( new FacadeMethod<>( "String[] nodeIndexNames()", IndexManager::nodeIndexNames ) ),
    EXISTS_FOR_RELATIONSHIPS( new FacadeMethod<>( "boolean existsForRelationships( String indexName )", self -> self.existsForRelationships( "foo" ) ) ),
    FOR_RELATIONSHIPS( new FacadeMethod<>( "RelationshipIndex forRelationships( String indexName )", self -> self.forRelationships( "foo" ) ) ),
    FOR_RELATIONSHIPS_WITH_CONFIG( new FacadeMethod<>( "RelationshipIndex forRelationships( String indexName, Map<String, String> customConfiguration )",
            self -> self.forRelationships( "foo", null ) ) ),
    RELATIONSHIP_INDEX_NAMES( new FacadeMethod<>( "String[] relationshipIndexNames()", IndexManager::relationshipIndexNames ) );

    private final FacadeMethod<IndexManager> facadeMethod;

    IndexManagerFacadeMethods( FacadeMethod<IndexManager> facadeMethod )
    {
        this.facadeMethod = facadeMethod;
    }

    @Override
    public void accept( IndexManager indexManager )
    {
        facadeMethod.accept( indexManager );
    }

    @Override
    public String toString()
    {
        return facadeMethod.toString();
    }
}
