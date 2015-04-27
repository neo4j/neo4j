/*
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
package org.neo4j.kernel.api.txstate;

import java.util.Map;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

/**
 * Kernel transaction state, please see {@link org.neo4j.kernel.impl.api.state.TxState} for implementation details.
 *
 * This interface defines the mutating methods for the transaction state, methods for reading are defined in
 * {@link ReadableTxState}. These mutating methods follow the rule that they all contain the word "Do" in the name.
 * This naming convention helps deciding where to set {@link #hasChanges()} in the
 * {@link org.neo4j.kernel.impl.api.state.TxState main implementation class}.
 */
public interface TransactionState extends ReadableTxState
{
    // ENTITY RELATED

    void relationshipDoCreate( long id, int relationshipTypeId, long startNodeId, long endNodeId );

    void nodeDoCreate( long id );

    void relationshipDoDelete( long relationshipId, int type, long startNode, long endNode );

    void relationshipDoDeleteAddedInThisTx( long relationshipId );

    void nodeDoDelete( long nodeId );

    void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty );

    void relationshipDoReplaceProperty( long relationshipId,
                                        Property replacedProperty, DefinedProperty newProperty );

    void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty );

    void nodeDoRemoveProperty( long nodeId, DefinedProperty removedProperty );

    void relationshipDoRemoveProperty( long relationshipId, DefinedProperty removedProperty );

    void graphDoRemoveProperty( DefinedProperty removedProperty );

    void nodeDoAddLabel( int labelId, long nodeId );

    void nodeDoRemoveLabel( int labelId, long nodeId );

    // TOKEN RELATED

    void labelDoCreateForName( String labelName, int id );

    void propertyKeyDoCreateForName( String propertyKeyName, int id );

    void relationshipTypeDoCreateForName( String relationshipTypeName, int id );

    // SCHEMA RELATED

    void indexRuleDoAdd( IndexDescriptor descriptor );

    void constraintIndexRuleDoAdd( IndexDescriptor descriptor );

    void indexDoDrop( IndexDescriptor descriptor );

    void constraintIndexDoDrop( IndexDescriptor descriptor );

    void constraintDoAdd( UniquenessConstraint constraint, long indexId );

    void constraintDoDrop( UniquenessConstraint constraint );

    boolean constraintDoUnRemove( UniquenessConstraint constraint );

    boolean constraintIndexDoUnRemove( IndexDescriptor index );

    // <Legacy index>
    void nodeLegacyIndexDoCreate( String indexName, Map<String, String> customConfig );

    void relationshipLegacyIndexDoCreate( String indexName, Map<String, String> customConfig );
    // </Legacy index>

    void indexDoUpdateProperty( IndexDescriptor descriptor, long nodeId, DefinedProperty before, DefinedProperty after );
}
