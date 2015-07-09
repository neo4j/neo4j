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

package org.neo4j.kernel.impl.api;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.api.constraints.MandatoryNodePropertyConstraint;
import org.neo4j.kernel.api.constraints.MandatoryRelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.MandatoryNodePropertyConstraintViolationKernelException;
import org.neo4j.kernel.api.exceptions.schema.MandatoryRelationshipPropertyConstraintViolationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class MandatoryPropertyEnforcer extends TxStateVisitor.Adapter
{
    private final EntityReadOperations readOperations;
    private final StoreReadLayer storeLayer;
    private final StoreStatement storeStatement;
    private final TxStateHolder txStateHolder;

    public MandatoryPropertyEnforcer( EntityReadOperations operations,
            TxStateVisitor next,
            TxStateHolder txStateHolder,
            StoreReadLayer storeLayer,
            StoreStatement storeStatement )
    {
        super( next );
        this.readOperations = operations;
        this.txStateHolder = txStateHolder;
        this.storeLayer = storeLayer;
        this.storeStatement = storeStatement;
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
            Iterator<Integer> removed ) throws ConstraintValidationKernelException
    {
        validateNode( id );
        super.visitNodePropertyChanges( id, added, changed, removed );
    }

    @Override
    public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
            throws ConstraintValidationKernelException
    {
        validateNode( id );
        super.visitNodeLabelChanges( id, added, removed );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            throws ConstraintValidationKernelException
    {
        validateRelationship( id, type );
        super.visitCreatedRelationship( id, type, startNode, endNode );
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
            Iterator<Integer> removed ) throws ConstraintValidationKernelException
    {
        validateRelationship( id );
        super.visitRelPropertyChanges( id, added, changed, removed );
    }

    private void validateNode( long node ) throws ConstraintValidationKernelException
    {
        for ( PrimitiveIntIterator labels = labelsOf( node ); labels.hasNext(); )
        {
            for ( NodePropertyConstraint constraint : loop( mandatoryNodePropertyConstraints( labels.next() ) ) )
            {
                if ( !nodeHasProperty( node, constraint.propertyKey() ) )
                {
                    throw new MandatoryNodePropertyConstraintViolationKernelException( constraint.label(),
                            constraint.propertyKey(), node );
                }
            }
        }
    }

    private void validateRelationship( long id ) throws ConstraintValidationKernelException
    {
        try
        {
            int type = readOperations.relationshipGetType( txStateHolder, storeStatement, id );
            validateRelationship( id, type );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Relationship with changes should exist.", e );
        }
    }

    private void validateRelationship( long id, int type ) throws ConstraintValidationKernelException
    {
        Iterator<RelationshipPropertyConstraint> constraints = mandatoryRelPropertyConstraints( type );
        while ( constraints.hasNext() )
        {
            RelationshipPropertyConstraint constraint = constraints.next();
            if ( !relHasProperty( id, constraint.propertyKey() ) )
            {
                throw new MandatoryRelationshipPropertyConstraintViolationKernelException(
                        constraint.relationshipType(), constraint.propertyKey(), id );
            }
        }
    }

    private boolean nodeHasProperty( long nodeId, int propertyKeyId )
    {
        try
        {
            return readOperations.nodeHasProperty( txStateHolder, storeStatement, nodeId, propertyKeyId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Node with changes should exist.", e );
        }
    }

    private boolean relHasProperty( long relId, int propertyKeyId )
    {
        try
        {
            return readOperations.relationshipHasProperty( txStateHolder, storeStatement, relId, propertyKeyId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Relationship with changes should exist.", e );
        }
    }

    private PrimitiveIntIterator labelsOf( long nodeId )
    {
        try
        {
            return readOperations.nodeGetLabels( txStateHolder, storeStatement, nodeId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Node with changes should exist.", e );
        }
    }

    private Iterator<NodePropertyConstraint> mandatoryNodePropertyConstraints( int label )
    {
        return new FilteringIterator<>( storeLayer.constraintsGetForLabel( label ),
                new Predicate<NodePropertyConstraint>()
                {
                    @Override
                    public boolean test( NodePropertyConstraint constraint )
                    {
                        return constraint instanceof MandatoryNodePropertyConstraint;
                    }
                } );
    }

    private Iterator<RelationshipPropertyConstraint> mandatoryRelPropertyConstraints( int type )
    {
        return new FilteringIterator<>( storeLayer.constraintsGetForRelationshipType( type ),
                new Predicate<RelationshipPropertyConstraint>()
                {
                    @Override
                    public boolean test( RelationshipPropertyConstraint constraint )
                    {
                        return constraint instanceof MandatoryRelationshipPropertyConstraint;
                    }
                } );
    }
}
