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

import java.util.Iterator;
import java.util.Set;

import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

public class TransactionToRecordStateVisitor extends TxStateVisitor.Adapter
{
    private boolean clearSchemaState;
    private final TransactionRecordState recordState;
    private final SchemaState schemaState;
    private final SchemaStorage schemaStorage;
    private final ConstraintSemantics constraintSemantics;
    private final IndexProviderMap indexProviderMap;

    public TransactionToRecordStateVisitor( TransactionRecordState recordState, SchemaState schemaState,
                                            SchemaStorage schemaStorage, ConstraintSemantics constraintSemantics,
                                            IndexProviderMap indexProviderMap )
    {
        this.recordState = recordState;
        this.schemaState = schemaState;
        this.schemaStorage = schemaStorage;
        this.constraintSemantics = constraintSemantics;
        this.indexProviderMap = indexProviderMap;
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
            Iterator<StorageProperty> changed, Iterator<Integer> removed )
    {
        while ( removed.hasNext() )
        {
            recordState.nodeRemoveProperty( id, removed.next() );
        }
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
            Iterator<StorageProperty> changed, Iterator<Integer> removed )
    {
        while ( removed.hasNext() )
        {
            recordState.relRemoveProperty( id, removed.next() );
        }
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
            Iterator<Integer> removed )
    {
        while ( removed.hasNext() )
        {
            recordState.graphRemoveProperty( removed.next() );
        }
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
    public void visitNodeLabelChanges( long id, final Set<Integer> added, final Set<Integer> removed )
    {
        // record the state changes to be made to the store
        for ( Integer label : removed )
        {
            recordState.removeLabelFromNode( label, id );
        }
        for ( Integer label : added )
        {
            recordState.addLabelToNode( label, id );
        }
    }

    @Override
    public void visitAddedIndex( SchemaIndexDescriptor index, IndexProvider.Descriptor providerDescriptor )
    {
        if ( providerDescriptor == null )
        {
            // No specific provider descriptor, use the default
            providerDescriptor = indexProviderMap.getDefaultProvider().getProviderDescriptor();
        }
        else if ( indexProviderMap.lookup( providerDescriptor ) == null )
        {
            // A specific provider descriptor, verify that it exists
            throw new IllegalArgumentException( "Specified non-existent provider '" + providerDescriptor + "' for created index " + index );
        }

        IndexRule rule = IndexRule.indexRule( schemaStorage.newRuleId(), index, providerDescriptor );
        recordState.createSchemaRule( rule );
    }

    @Override
    public void visitRemovedIndex( SchemaIndexDescriptor index )
    {
        IndexRule rule = schemaStorage.indexGetForSchema( index );
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
        IndexRule indexRule = schemaStorage.indexGetForSchema( uniqueConstraint.ownedIndexDescriptor() );
        recordState.createSchemaRule( constraintSemantics.createUniquenessConstraintRule(
                constraintId, uniqueConstraint, indexRule.getId() ) );
        recordState.setConstraintIndexOwner( indexRule, constraintId );
    }

    private void visitAddedNodeKeyConstraint( NodeKeyConstraintDescriptor uniqueConstraint, long constraintId )
            throws CreateConstraintFailureException
    {
        IndexRule indexRule = schemaStorage.indexGetForSchema( uniqueConstraint.ownedIndexDescriptor() );
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
    public void visitCreatedLabelToken( String name, int id )
    {
        recordState.createLabelToken( name, id );
    }

    @Override
    public void visitCreatedPropertyKeyToken( String name, int id )
    {
        recordState.createPropertyKeyToken( name, id );
    }

    @Override
    public void visitCreatedRelationshipTypeToken( String name, int id )
    {
        recordState.createRelationshipTypeToken( name, id );
    }

}
