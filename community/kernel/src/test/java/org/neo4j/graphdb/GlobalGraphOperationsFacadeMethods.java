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

import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class GlobalGraphOperationsFacadeMethods
{
    private static final FacadeMethod<GlobalGraphOperations> GET_ALL_NODES =
        new FacadeMethod<GlobalGraphOperations>( "Iterable<Node> getAllNodes()" )
        {
            @Override
            public void call( GlobalGraphOperations self )
            {
                self.getAllNodes();
            }
        };

    private static final FacadeMethod<GlobalGraphOperations> GET_ALL_RELATIONSHIPS =
        new FacadeMethod<GlobalGraphOperations>( "Iterable<Relationship> getAllRelationships()" )
        {
            @Override
            public void call( GlobalGraphOperations self )
            {
                self.getAllRelationships();
            }
        };

    private static final FacadeMethod<GlobalGraphOperations> GET_ALL_RELATIONSHIP_TYPES =
        new FacadeMethod<GlobalGraphOperations>( "Iterable<RelationshipType> getAllRelationshipTypes()" )
        {
            @Override
            public void call( GlobalGraphOperations self )
            {
                self.getAllRelationshipTypes();
            }
        };

    private static final FacadeMethod<GlobalGraphOperations> GET_ALL_LABELS =
        new FacadeMethod<GlobalGraphOperations>( "ResourceIterable<Label> getAllLabels()" )
        {
            @Override
            public void call( GlobalGraphOperations self )
            {
                self.getAllLabels();
            }
        };

    private static final FacadeMethod<GlobalGraphOperations> GET_ALL_NODES_WITH_LABEL =
        new FacadeMethod<GlobalGraphOperations>( "ResourceIterable<Node> getAllNodesWithLabel( Label label )" )
        {
            @Override
            public void call( GlobalGraphOperations self )
            {
                self.getAllNodesWithLabel( DynamicLabel.label( "Label" ) );
            }
        };

    static final Iterable<FacadeMethod<GlobalGraphOperations>> ALL_GLOBAL_GRAPH_OPERATIONS_FACADE_METHODS =
        unmodifiableCollection( asList(
            GET_ALL_NODES,
            GET_ALL_RELATIONSHIPS,
            GET_ALL_RELATIONSHIP_TYPES,
            GET_ALL_LABELS,
            GET_ALL_NODES_WITH_LABEL
        ) );
}

