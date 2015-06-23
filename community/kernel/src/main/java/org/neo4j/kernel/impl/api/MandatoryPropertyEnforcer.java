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
import org.neo4j.kernel.api.constraints.MandatoryPropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.MandatoryPropertyConstraintViolationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;

import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.api.StateHandlingStatementOperations.nodeGetProperty;

public class MandatoryPropertyEnforcer extends TxStateVisitor.Adapter
{
    private final StoreReadLayer storeLayer;
    private final TransactionState txState;

    public MandatoryPropertyEnforcer( TxStateVisitor next, StoreReadLayer storeLayer, TransactionState txState )
    {
        super( next );
        this.storeLayer = storeLayer;
        this.txState = txState;
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

    private void validateNode( long node ) throws ConstraintValidationKernelException
    {
        for ( PrimitiveIntIterator labels = labelsOf( node ); labels.hasNext(); )
        {
            for ( PropertyConstraint constraint : loop( mandatoryPropertyConstraints( labels.next() ) ) )
            {
                if ( !hasProperty( node, constraint.propertyKeyId() ) )
                {
                    throw new MandatoryPropertyConstraintViolationKernelException( constraint.label(),
                            constraint.propertyKeyId(), node );
                }
            }
        }
    }

    private boolean hasProperty( long nodeId, int propertyKeyId )
    {
        try ( StoreStatement statement = storeLayer.acquireStatement() )
        {
            return nodeGetProperty( storeLayer, statement, txState, nodeId, propertyKeyId ).isDefined();
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Node with changes should exist.", e );
        }
    }

    private PrimitiveIntIterator labelsOf( long nodeId )
    {
        try ( StoreStatement statement = storeLayer.acquireStatement() )
        {
            return StateHandlingStatementOperations.nodeGetLabels( storeLayer, statement, txState, nodeId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Node with changes should exist.", e );
        }
    }

    private Iterator<PropertyConstraint> mandatoryPropertyConstraints( int label )
    {
        return new FilteringIterator<>( storeLayer.constraintsGetForLabel( label ), new Predicate<PropertyConstraint>()
        {
            @Override
            public boolean test( PropertyConstraint constraint )
            {
                return constraint instanceof MandatoryPropertyConstraint;
            }
        } );
    }
}
