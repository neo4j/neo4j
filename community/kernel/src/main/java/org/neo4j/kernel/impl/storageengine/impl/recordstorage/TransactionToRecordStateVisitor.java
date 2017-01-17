/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
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
    private final Runnable schemaStateChangeCallback;
    private final SchemaStorage schemaStorage;
    private final ConstraintSemantics constraintSemantics;
    private final SchemaIndexProviderMap schemaIndexProviderMap;

    public TransactionToRecordStateVisitor( TransactionRecordState recordState, Runnable schemaStateChangeCallback,
                                            SchemaStorage schemaStorage, ConstraintSemantics constraintSemantics,
                                            SchemaIndexProviderMap schemaIndexProviderMap )
    {
        this.recordState = recordState;
        this.schemaStateChangeCallback = schemaStateChangeCallback;
        this.schemaStorage = schemaStorage;
        this.constraintSemantics = constraintSemantics;
        this.schemaIndexProviderMap = schemaIndexProviderMap;
    }

    @Override
    public void close()
    {
        try
        {
            if ( clearSchemaState )
            {
                schemaStateChangeCallback.run();
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
            DefinedProperty prop = (DefinedProperty) changed.next();
            recordState.nodeChangeProperty( id, prop.propertyKeyId(), prop.value() );
        }
        while ( added.hasNext() )
        {
            DefinedProperty prop = (DefinedProperty) added.next();
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
            DefinedProperty prop = (DefinedProperty) changed.next();
            recordState.relChangeProperty( id, prop.propertyKeyId(), prop.value() );
        }
        while ( added.hasNext() )
        {
            DefinedProperty prop = (DefinedProperty) added.next();
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
            DefinedProperty prop = (DefinedProperty) changed.next();
            recordState.graphChangeProperty( prop.propertyKeyId(), prop.value() );
        }
        while ( added.hasNext() )
        {
            DefinedProperty prop = (DefinedProperty) added.next();
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
    public void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex )
    {
        SchemaIndexProvider.Descriptor providerDescriptor =
                schemaIndexProviderMap.getDefaultProvider().getProviderDescriptor();
        IndexRule rule;
        if ( isConstraintIndex )
        {
            // was IndexRule.constraintIndexRule, but calling this with null gives the non-constraint version?
            rule = IndexRule.indexRule(
                        schemaStorage.newRuleId(),
                        NewIndexDescriptorFactory.uniqueForLabel( element.getLabelId(), element.getPropertyKeyId() ),
                        providerDescriptor );
        }
        else
        {
            rule = IndexRule.indexRule(
                        schemaStorage.newRuleId(),
                        NewIndexDescriptorFactory.forLabel( element.getLabelId(), element.getPropertyKeyId() ),
                        providerDescriptor );
        }
        recordState.createSchemaRule( rule );
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor desc, boolean isConstraintIndex )
    {
        SchemaStorage.IndexRuleKind kind = isConstraintIndex ?
                SchemaStorage.IndexRuleKind.CONSTRAINT
                : SchemaStorage.IndexRuleKind.INDEX;
        IndexRule rule = schemaStorage.indexRule(
                SchemaDescriptorFactory.forLabel( desc.getLabelId(), desc.getPropertyKeyId() ), kind );
        recordState.dropSchemaRule( rule );
    }

    @Override
    public void visitAddedUniquePropertyConstraint( UniquenessConstraint element )
    {
        clearSchemaState = true;
        long constraintId = schemaStorage.newRuleId();
        int propertyKeyId = element.descriptor().getPropertyKeyId();

        IndexRule indexRule = schemaStorage.indexRule(
                SchemaDescriptorFactory.forLabel( element.label(), propertyKeyId ),
                SchemaStorage.IndexRuleKind.CONSTRAINT );
        recordState.createSchemaRule(
                constraintSemantics.writeUniquePropertyConstraint(
                        constraintId, element.descriptor(), indexRule.getId() ) );
        recordState.setConstraintIndexOwner( indexRule, constraintId );
    }

    @Override
    public void visitRemovedUniquePropertyConstraint( UniquenessConstraint element )
    {
        try
        {
            clearSchemaState = true;
            int propertyKeyId = element.descriptor().getPropertyKeyId();

            recordState.dropSchemaRule( schemaStorage.singleConstraintRule(
                    ConstraintDescriptorFactory.uniqueForLabel( element.label(), propertyKeyId )
                ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IllegalStateException(
                    "Constraint to be removed should exist, since its existence should have been validated earlier " +
                    "and the schema should have been locked." );
        }
        catch ( DuplicateSchemaRuleException de )
        {
            throw new IllegalStateException( "Multiple constraints found for specified label and property." );
        }
        // Remove the index for the constraint as well
        visitRemovedIndex( element.indexDescriptor(), true );
    }

    @Override
    public void visitAddedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
            throws CreateConstraintFailureException
    {
        clearSchemaState = true;
        recordState.createSchemaRule( constraintSemantics.writeNodePropertyExistenceConstraint(
                schemaStorage.newRuleId(), element.descriptor() ) );
    }

    @Override
    public void visitRemovedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint element )
    {
        try
        {
            clearSchemaState = true;
            int propertyKeyId = element.descriptor().getPropertyKeyId();

            recordState.dropSchemaRule( schemaStorage.singleConstraintRule(
                    ConstraintDescriptorFactory.existsForLabel( element.label(), propertyKeyId )
                ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IllegalStateException(
                    "Node property existence constraint to be removed should exist, since its existence should " +
                    "have been validated earlier and the schema should have been locked." );
        }
        catch ( DuplicateSchemaRuleException de )
        {
            throw new IllegalStateException( "Multiple node property constraints found for specified label and " +
                                             "property." );
        }
    }

    @Override
    public void visitAddedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
            throws CreateConstraintFailureException
    {
        clearSchemaState = true;
        recordState.createSchemaRule( constraintSemantics.writeRelationshipPropertyExistenceConstraint(
                schemaStorage.newRuleId(), element.descriptor() ) );
    }

    @Override
    public void visitRemovedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint element )
    {
        try
        {
            //TODO: Support composite indexes
            clearSchemaState = true;

            recordState.dropSchemaRule( schemaStorage.singleConstraintRule(
                    ConstraintDescriptorFactory.existsForRelType(
                            element.descriptor().getRelationshipTypeId(), element.descriptor().getPropertyKeyId() )
                ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IllegalStateException(
                    "Relationship property existence constraint to be removed should exist, since its existence " +
                    "should have been validated earlier and the schema should have been locked." );
        }
        catch ( DuplicateSchemaRuleException re )
        {
            throw new IllegalStateException( "Multiple relationship property constraints found for specified " +
                                             "property and relationship type." );
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
