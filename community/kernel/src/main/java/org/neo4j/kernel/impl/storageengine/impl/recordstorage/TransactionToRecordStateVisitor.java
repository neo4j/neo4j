/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.Iterator;
import java.util.Optional;

import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

class TransactionToRecordStateVisitor extends TxStateVisitor.Adapter
{
    private boolean clearSchemaState;
    private final TransactionRecordState recordState;
    private final SchemaState schemaState;
    private final SchemaStorage schemaStorage;
    private final ConstraintSemantics constraintSemantics;

    TransactionToRecordStateVisitor( TransactionRecordState recordState, SchemaState schemaState, SchemaStorage schemaStorage,
            ConstraintSemantics constraintSemantics )
    {
        this.recordState = recordState;
        this.schemaState = schemaState;
        this.schemaStorage = schemaStorage;
        this.constraintSemantics = constraintSemantics;
    }

    @Override
    public void close()
    {
        try
        {
            if ( clearSchemaState )
            {
                schemaState.clear();
            }
        }
        finally
        {
            clearSchemaState = false;
        }
    }

    @Override
    public void visitCreatedNode( long id )
    {
        recordState.nodeCreate( id );
    }

    @Override
    public void visitDeletedNode( long id )
    {
        recordState.nodeDelete( id );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
    {
        // record the state changes to be made to the store
        recordState.relCreate( id, type, startNode, endNode );
    }

    @Override
    public void visitDeletedRelationship( long id )
    {
        // record the state changes to be made to the store
        recordState.relDelete( id );
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added,
            Iterator<StorageProperty> changed, IntIterable removed )
    {
        removed.each( propId -> recordState.nodeRemoveProperty( id, propId ) );
        while ( changed.hasNext() )
        {
            StorageProperty prop = changed.next();
            recordState.nodeChangeProperty( id, prop.propertyKeyId(), prop.value() );
        }
        while ( added.hasNext() )
        {
            StorageProperty prop = added.next();
            recordState.nodeAddProperty( id, prop.propertyKeyId(), prop.value() );
        }
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added,
            Iterator<StorageProperty> changed, IntIterable removed )
    {
        removed.each( relId -> recordState.relRemoveProperty( id, relId ) );
        while ( changed.hasNext() )
        {
            StorageProperty prop = changed.next();
            recordState.relChangeProperty( id, prop.propertyKeyId(), prop.value() );
        }
        while ( added.hasNext() )
        {
            StorageProperty prop = added.next();
            recordState.relAddProperty( id, prop.propertyKeyId(), prop.value() );
        }
    }

    @Override
    public void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
            IntIterable removed )
    {
        removed.each( recordState::graphRemoveProperty );
        while ( changed.hasNext() )
        {
            StorageProperty prop = changed.next();
            recordState.graphChangeProperty( prop.propertyKeyId(), prop.value() );
        }
        while ( added.hasNext() )
        {
            StorageProperty prop = added.next();
            recordState.graphAddProperty( prop.propertyKeyId(), prop.value() );
        }
    }

    @Override
    public void visitNodeLabelChanges( long id, final LongSet added, final LongSet removed )
    {
        // record the state changes to be made to the store
        removed.each( label -> recordState.removeLabelFromNode( label, id ) );
        added.each( label -> recordState.addLabelToNode( label, id ) );
    }

    @Override
    public void visitAddedIndex( IndexDescriptor index )
    {
        StoreIndexDescriptor rule = index.withId( schemaStorage.newRuleId() );
        recordState.createSchemaRule( rule );
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor index )
    {
        StoreIndexDescriptor rule = null;
        Optional<String> name = index.getUserSuppliedName();
        if ( name.isPresent() )
        {
            String indexName = name.get();
            rule = schemaStorage.indexGetForName( indexName );
        }
        else
        {
            rule = schemaStorage.indexGetForSchema( index );
        }
        if ( rule != null )
        {
            recordState.dropSchemaRule( rule );
        }
    }

    @Override
    public void visitAddedConstraint( ConstraintDescriptor constraint ) throws CreateConstraintFailureException
    {
        clearSchemaState = true;
        long constraintId = schemaStorage.newRuleId();

        switch ( constraint.type() )
        {
        case UNIQUE:
            visitAddedUniquenessConstraint( (UniquenessConstraintDescriptor) constraint, constraintId );
            break;

        case UNIQUE_EXISTS:
            visitAddedNodeKeyConstraint( (NodeKeyConstraintDescriptor) constraint, constraintId );
            break;

        case EXISTS:
            recordState.createSchemaRule(
                    constraintSemantics.createExistenceConstraint( schemaStorage.newRuleId(), constraint ) );
            break;

        default:
            throw new IllegalStateException( constraint.type().toString() );
        }
    }

    private void visitAddedUniquenessConstraint( UniquenessConstraintDescriptor uniqueConstraint, long constraintId )
    {
        StoreIndexDescriptor indexRule = schemaStorage.indexGetForSchema( uniqueConstraint.ownedIndexDescriptor() );
        recordState.createSchemaRule( constraintSemantics.createUniquenessConstraintRule(
                constraintId, uniqueConstraint, indexRule.getId() ) );
        recordState.setConstraintIndexOwner( indexRule, constraintId );
    }

    private void visitAddedNodeKeyConstraint( NodeKeyConstraintDescriptor uniqueConstraint, long constraintId )
            throws CreateConstraintFailureException
    {
        StoreIndexDescriptor indexRule = schemaStorage.indexGetForSchema( uniqueConstraint.ownedIndexDescriptor() );
        recordState.createSchemaRule( constraintSemantics.createNodeKeyConstraintRule(
                constraintId, uniqueConstraint, indexRule.getId() ) );
        recordState.setConstraintIndexOwner( indexRule, constraintId );
    }

    @Override
    public void visitRemovedConstraint( ConstraintDescriptor constraint )
    {
        clearSchemaState = true;
        try
        {
            recordState.dropSchemaRule( schemaStorage.constraintsGetSingle( constraint ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IllegalStateException(
                    "Constraint to be removed should exist, since its existence should have been validated earlier " +
                            "and the schema should have been locked." );
        }
        catch ( DuplicateSchemaRuleException e )
        {
            throw new IllegalStateException( "Multiple constraints found for specified label and property." );
        }
        if ( constraint.enforcesUniqueness() )
        {
            // Remove the index for the constraint as well
            visitRemovedIndex( ((IndexBackedConstraintDescriptor)constraint).ownedIndexDescriptor() );
        }
    }

    @Override
    public void visitCreatedLabelToken( long id, String name )
    {
        recordState.createLabelToken( name, id );
    }

    @Override
    public void visitCreatedPropertyKeyToken( long id, String name )
    {
        recordState.createPropertyKeyToken( name, id );
    }

    @Override
    public void visitCreatedRelationshipTypeToken( long id, String name )
    {
        recordState.createRelationshipTypeToken( name, id );
    }

}
