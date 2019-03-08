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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.storageengine.api.ConstraintRule;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

class TransactionToRecordStateVisitor extends TxStateVisitor.Adapter
{
    private boolean clearSchemaState;
    private final TransactionRecordState recordState;
    private final SchemaState schemaState;
    private final SchemaRuleAccess schemaStorage;
    private final SchemaRecordChangeTranslator schemaStateChanger;
    private final ConstraintRuleAccessor constraintSemantics;

    TransactionToRecordStateVisitor( TransactionRecordState recordState, SchemaState schemaState, SchemaRuleAccess schemaRuleAccess,
            ConstraintRuleAccessor constraintSemantics )
    {
        this.recordState = recordState;
        this.schemaState = schemaState;
        this.schemaStorage = schemaRuleAccess;
        this.schemaStateChanger = schemaRuleAccess.getSchemaRecordChangeTranslator();
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
    public void visitAddedIndex( IndexDescriptor index ) throws KernelException
    {
        SchemaRule rule = new DefaultStorageIndexReference( index, schemaStorage.newRuleId(), null );
        schemaStateChanger.createSchemaRule( recordState, rule );
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor index )
    {
        StorageIndexReference rule = null;
        if ( index instanceof StorageIndexReference )
        {
            rule = (StorageIndexReference) index;
        }
        else if ( index.hasUserSuppliedName() )
        {
            String indexName = index.name();
            rule = schemaStorage.indexGetForName( indexName );
        }
        else
        {
            // TODO we'll need to rethink this whole thing once multiple identical schemas are allowed.
            StorageIndexReference[] rules = schemaStorage.indexGetForSchema( index, true );
            if ( rules.length == 0 )
            {
                // Loosen the filtering a bit. The reason we do this during drop is this scenario where a uniqueness constraint creation
                // crashed or similar, where the UNIQUE index exists, but not its constraint and so the only way to drop it
                // (if you don't want to go the route of first creating a constraint and then drop that, where the index would be dropped along with it),
                // is to do "DROP INDEX ON :Label(name) which has the type as GENERAL and would miss it.
                rules = schemaStorage.indexGetForSchema( index, false );
            }
            if ( rules.length > 1 )
            {
                throw new IllegalStateException( "More than one index matched schema '" + index +
                        "', don't know which one to remove: " + Arrays.toString( rules ) );
            }
            if ( rules.length > 0 )
            {
                rule = rules[0];
            }
        }
        if ( rule != null )
        {
            schemaStateChanger.dropSchemaRule( recordState, rule );
        }
    }

    @Override
    public void visitAddedConstraint( ConstraintDescriptor constraint ) throws KernelException
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
            ConstraintRule rule = constraintSemantics.createExistenceConstraint( schemaStorage.newRuleId(), constraint );
            schemaStateChanger.createSchemaRule( recordState, rule );
            break;

        default:
            throw new IllegalStateException( constraint.type().toString() );
        }
    }

    private void visitAddedUniquenessConstraint( UniquenessConstraintDescriptor uniqueConstraint, long constraintId ) throws KernelException
    {
        StorageIndexReference indexRule = firstUniqueConstraintIndex( schemaStorage.indexGetForSchema( uniqueConstraint.ownedIndexDescriptor() ) );
        ConstraintRule constraintRule = constraintSemantics.createUniquenessConstraintRule( constraintId, uniqueConstraint, indexRule.getId() );
        schemaStateChanger.createSchemaRule( recordState, constraintRule );
        schemaStateChanger.setConstraintIndexOwner( recordState, indexRule, constraintId );
    }

    private void visitAddedNodeKeyConstraint( NodeKeyConstraintDescriptor uniqueConstraint, long constraintId ) throws KernelException
    {
        StorageIndexReference indexRule = firstUniqueConstraintIndex( schemaStorage.indexGetForSchema( uniqueConstraint.ownedIndexDescriptor() ) );
        ConstraintRule constraintRule = constraintSemantics.createNodeKeyConstraintRule( constraintId, uniqueConstraint, indexRule.getId() );
        schemaStateChanger.createSchemaRule( recordState, constraintRule );
        schemaStateChanger.setConstraintIndexOwner( recordState, indexRule, constraintId );
    }

    private StorageIndexReference firstUniqueConstraintIndex( StorageIndexReference[] indexGetForSchema )
    {
        return Stream.of( indexGetForSchema ).filter( StorageIndexReference::isUnique ).findFirst().orElse( null );
    }

    @Override
    public void visitRemovedConstraint( ConstraintDescriptor constraint )
    {
        clearSchemaState = true;
        try
        {
            ConstraintRule rule = schemaStorage.constraintsGetSingle( constraint );
            schemaStateChanger.dropSchemaRule( recordState, rule );
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
    public void visitCreatedLabelToken( long id, String name, boolean internal )
    {
        recordState.createLabelToken( name, id, internal );
    }

    @Override
    public void visitCreatedPropertyKeyToken( long id, String name, boolean internal )
    {
        recordState.createPropertyKeyToken( name, id, internal );
    }

    @Override
    public void visitCreatedRelationshipTypeToken( long id, String name, boolean internal )
    {
        recordState.createRelationshipTypeToken( name, id, internal );
    }
}
