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
package org.neo4j.graphdb;

import org.neo4j.graphdb.index.IndexManager;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class IndexManagerFacadeMethods
{
    private static final FacadeMethod<IndexManager> EXISTS_FOR_NODES = new FacadeMethod<IndexManager>( "boolean " +
            "existsForNodes( String indexName )" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            indexManager.existsForNodes( "foo" );
        }
    };

    private static final FacadeMethod<IndexManager> FOR_NODES = new FacadeMethod<IndexManager>( "Index<Node> forNodes" +
            "( String indexName )" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            indexManager.forNodes( "foo" );
        }
    };

    private static final FacadeMethod<IndexManager> FOR_NODES_WITH_CONFIGURATION = new FacadeMethod<IndexManager>(
            "Index<Node> forNodes( String indexName, Map<String, String> customConfiguration )" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            indexManager.forNodes( "foo", null );
        }
    };

    private static final FacadeMethod<IndexManager> NODE_INDEX_NAMES = new FacadeMethod<IndexManager>( "String[] " +
            "nodeIndexNames()" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            for ( String indexName : indexManager.nodeIndexNames() )
            {

            }
        }
    };

    private static final FacadeMethod<IndexManager> EXISTS_FOR_RELATIONSHIPS = new FacadeMethod<IndexManager>(
            "boolean existsForRelationships( String indexName )" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            indexManager.existsForRelationships( "foo" );
        }
    };

    private static final FacadeMethod<IndexManager> FOR_RELATIONSHIPS = new FacadeMethod<IndexManager>(
            "RelationshipIndex forRelationships( String indexName )" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            indexManager.forRelationships( "foo" );
        }
    };

    private static final FacadeMethod<IndexManager> FOR_RELATIONSHIPS_WITH_CONFIGURATION = new
            FacadeMethod<IndexManager>
            ( "RelationshipIndex forRelationships( String indexName, Map<String, String> customConfiguration )" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            indexManager.forRelationships( "foo", null );
        }
    };

    private static final FacadeMethod<IndexManager> RELATIONSHIP_INDEX_NAMES = new FacadeMethod<IndexManager>(
            "String[] relationshipIndexNames()" )
    {
        @Override
        public void call( IndexManager indexManager )
        {
            for ( String indexName : indexManager.relationshipIndexNames() )
            {

            }
        }
    };

    static final Iterable<FacadeMethod<IndexManager>> ALL_INDEX_MANAGER_FACADE_METHODS =
            unmodifiableCollection( asList(
                    EXISTS_FOR_NODES,
                    FOR_NODES,
                    FOR_NODES_WITH_CONFIGURATION,
                    NODE_INDEX_NAMES,
                    EXISTS_FOR_RELATIONSHIPS,
                    FOR_RELATIONSHIPS,
                    FOR_RELATIONSHIPS_WITH_CONFIGURATION,
                    RELATIONSHIP_INDEX_NAMES
            ) );
}