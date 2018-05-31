/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.enterprise;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.cursor.Cursor;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceException;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.collection.PrimitiveArrays.union;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VALIDATION;

class PropertyExistenceEnforcer
{
    static PropertyExistenceEnforcer getOrCreatePropertyExistenceEnforcerFrom( StorageReader storageReader )
    {
        return storageReader.getOrCreateSchemaDependantState( PropertyExistenceEnforcer.class, FACTORY );
    }

    private final List<LabelSchemaDescriptor> nodeConstraints;
    private final List<RelationTypeSchemaDescriptor> relationshipConstraints;
    private final MutableLongObjectMap<int[]> mandatoryNodePropertiesByLabel = new LongObjectHashMap<>();
    private final MutableLongObjectMap<int[]> mandatoryRelationshipPropertiesByType = new LongObjectHashMap<>();

    private PropertyExistenceEnforcer( List<LabelSchemaDescriptor> nodes, List<RelationTypeSchemaDescriptor> rels )
    {
        this.nodeConstraints = nodes;
        this.relationshipConstraints = rels;
        for ( LabelSchemaDescriptor constraint : nodes )
        {
            update( mandatoryNodePropertiesByLabel, constraint.getLabelId(),
                    copyAndSortPropertyIds( constraint.getPropertyIds() ) );
        }
        for ( RelationTypeSchemaDescriptor constraint : rels )
        {
            update( mandatoryRelationshipPropertiesByType, constraint.getRelTypeId(),
                    copyAndSortPropertyIds( constraint.getPropertyIds() ) );
        }
    }

    private static void update( MutableLongObjectMap<int[]> map, int key, int[] sortedValues )
    {
        int[] current = map.get( key );
        if ( current != null )
        {
            sortedValues = union( current, sortedValues );
        }
        map.put( key, sortedValues );
    }

    private static int[] copyAndSortPropertyIds( int[] propertyIds )
    {
        int[] values = new int[propertyIds.length];
        System.arraycopy( propertyIds, 0, values, 0, propertyIds.length );
        Arrays.sort( values );
        return values;
    }

    TxStateVisitor decorate( TxStateVisitor visitor, ReadableTransactionState txState, StorageReader storageReader )
    {
        return new Decorator( visitor, txState, storageReader );
    }

    private static final PropertyExistenceEnforcer NO_CONSTRAINTS = new PropertyExistenceEnforcer(
            emptyList(), emptyList() )
    {
        @Override
        TxStateVisitor decorate( TxStateVisitor visitor, ReadableTransactionState txState, StorageReader storageReader )
        {
            return visitor;
        }
    };
    private static final Function<StorageReader,PropertyExistenceEnforcer> FACTORY = storageReader ->
    {
        List<LabelSchemaDescriptor> nodes = new ArrayList<>();
        List<RelationTypeSchemaDescriptor> relationships = new ArrayList<>();
        for ( Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetAll(); constraints.hasNext(); )
        {
            ConstraintDescriptor constraint = constraints.next();
            if ( constraint.enforcesPropertyExistence() )
            {
                constraint.schema().processWith( new SchemaProcessor()
                {
                    @Override
                    public void processSpecific( LabelSchemaDescriptor schema )
                    {
                        nodes.add( schema );
                    }

                    @Override
                    public void processSpecific( RelationTypeSchemaDescriptor schema )
                    {
                        relationships.add( schema );
                    }
                } );
            }
        }
        if ( nodes.isEmpty() && relationships.isEmpty() )
        {
            return NO_CONSTRAINTS;
        }
        return new PropertyExistenceEnforcer( nodes, relationships );
    };

    private class Decorator extends TxStateVisitor.Delegator
    {
        private final ReadableTransactionState txState;
        private final MutableIntSet propertyKeyIds = new IntHashSet();
        private final StorageReader storageReader;

        Decorator( TxStateVisitor next, ReadableTransactionState txState, StorageReader storageReader )
        {
            super( next );
            this.txState = txState;
            this.storageReader = storageReader;
        }

        @Override
        public void visitNodePropertyChanges(
                long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                IntIterable removed ) throws ConstraintValidationException
        {
            validateNode( id );
            super.visitNodePropertyChanges( id, added, changed, removed );
        }

        @Override
        public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
                throws ConstraintValidationException
        {
            validateNode( id );
            super.visitNodeLabelChanges( id, added, removed );
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
                throws ConstraintValidationException
        {
            validateRelationship( id );
            super.visitCreatedRelationship( id, type, startNode, endNode );
        }

        @Override
        public void visitRelPropertyChanges(
                long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                IntIterable removed ) throws ConstraintValidationException
        {
            validateRelationship( id );
            super.visitRelPropertyChanges( id, added, changed, removed );
        }

        private void validateNode( long nodeId ) throws NodePropertyExistenceException
        {
            if ( mandatoryNodePropertiesByLabel.isEmpty() )
            {
                return;
            }

            final LongSet labelIds;
            try ( Cursor<NodeItem> node = node( nodeId ) )
            {
                if ( node.next() )
                {
                    labelIds = node.get().labels();
                    if ( labelIds.isEmpty() )
                    {
                        return;
                    }
                    propertyKeyIds.clear();
                    try ( Cursor<PropertyItem> properties = properties( node.get() ) )
                    {
                        while ( properties.next() )
                        {
                            propertyKeyIds.add( properties.get().propertyKeyId() );
                        }
                    }
                }
                else
                {
                    throw new IllegalStateException( format( "Node %d with changes should exist.", nodeId ) );
                }
            }

            validateNodeProperties( nodeId, labelIds, propertyKeyIds );
        }

        private void validateRelationship( long id ) throws RelationshipPropertyExistenceException
        {
            if ( mandatoryRelationshipPropertiesByType.isEmpty() )
            {
                return;
            }

            int relationshipType;
            int[] required;
            try ( Cursor<RelationshipItem> relationship = relationship( id ) )
            {
                if ( relationship.next() )
                {
                    relationshipType = relationship.get().type();
                    required = mandatoryRelationshipPropertiesByType.get( relationshipType );
                    if ( required == null )
                    {
                        return;
                    }
                    propertyKeyIds.clear();
                    try ( Cursor<PropertyItem> properties = properties( relationship.get() ) )
                    {
                        while ( properties.next() )
                        {
                            propertyKeyIds.add( properties.get().propertyKeyId() );
                        }
                    }
                }
                else
                {
                    throw new IllegalStateException( format( "Relationship %d with changes should exist.", id ) );
                }
            }

            for ( int mandatory : required )
            {
                if ( !propertyKeyIds.contains( mandatory ) )
                {
                    failRelationship( id, relationshipType, mandatory );
                }
            }
        }

        private Cursor<NodeItem> node( long id )
        {
            Cursor<NodeItem> cursor = storageReader.acquireSingleNodeCursor( id );
            return txState.augmentSingleNodeCursor( cursor, id );
        }

        private Cursor<RelationshipItem> relationship( long id )
        {
            Cursor<RelationshipItem> cursor = storageReader.acquireSingleRelationshipCursor( id );
            return txState.augmentSingleRelationshipCursor( cursor, id );
        }

        private Cursor<PropertyItem> properties( NodeItem node )
        {
            Lock lock = node.lock();
            Cursor<PropertyItem> cursor = storageReader.acquirePropertyCursor( node.nextPropertyId(), lock,
                    AssertOpen.ALWAYS_OPEN );
            return txState.augmentPropertyCursor( cursor, txState.getNodeState( node.id() ) );
        }

        private Cursor<PropertyItem> properties( RelationshipItem relationship )
        {
            Lock lock = relationship.lock();
            Cursor<PropertyItem> cursor = storageReader.acquirePropertyCursor( relationship.nextPropertyId(), lock,
                    AssertOpen.ALWAYS_OPEN );
            return txState.augmentPropertyCursor( cursor, txState.getRelationshipState( relationship.id() ) );
        }
    }

    private void validateNodeProperties( long id, LongSet labelIds, IntSet propertyKeyIds )
            throws NodePropertyExistenceException
    {
        if ( labelIds.size() > mandatoryNodePropertiesByLabel.size() )
        {
            for ( MutableLongIterator labels = mandatoryNodePropertiesByLabel.keySet().longIterator(); labels.hasNext(); )
            {
                final long label = labels.next();
                if ( labelIds.contains( label ) )
                {
                    validateNodeProperties( id, label, mandatoryNodePropertiesByLabel.get( label ), propertyKeyIds );
                }
            }
        }
        else
        {
            for ( LongIterator labels = labelIds.longIterator(); labels.hasNext(); )
            {
                final long label = labels.next();
                int[] keys = mandatoryNodePropertiesByLabel.get( label );
                if ( keys != null )
                {
                    validateNodeProperties( id, label, keys, propertyKeyIds );
                }
            }
        }
    }

    private void validateNodeProperties( long id, long label, int[] requiredKeys, IntSet propertyKeyIds )
            throws NodePropertyExistenceException
    {
        for ( int key : requiredKeys )
        {
            if ( !propertyKeyIds.contains( key ) )
            {
                failNode( id, label, key );
            }
        }
    }

    private void failNode( long id, long label, int propertyKey )
            throws NodePropertyExistenceException
    {
        for ( LabelSchemaDescriptor constraint : nodeConstraints )
        {
            if ( constraint.getLabelId() == label && contains( constraint.getPropertyIds(), propertyKey ) )
            {
                throw new NodePropertyExistenceException( constraint, VALIDATION, id );
            }
        }
        throw new IllegalStateException( format(
                "Node constraint for label=%d, propertyKey=%d should exist.",
                label, propertyKey ) );
    }

    private void failRelationship( long id, int relationshipType, int propertyKey )
            throws RelationshipPropertyExistenceException
    {
        for ( RelationTypeSchemaDescriptor constraint : relationshipConstraints )
        {
            if ( constraint.getRelTypeId() == relationshipType && contains( constraint.getPropertyIds(), propertyKey ) )
            {
                throw new RelationshipPropertyExistenceException( constraint, VALIDATION, id );
            }
        }
        throw new IllegalStateException( format(
                "Relationship constraint for relationshipType=%d, propertyKey=%d should exist.",
                relationshipType, propertyKey ) );
    }

    private boolean contains( int[] list, int value )
    {
        for ( int x : list )
        {
            if ( value == x )
            {
                return true;
            }
        }
        return false;
    }
}
