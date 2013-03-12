/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

/**
 * Interface for accessing and modifying the underlying graph.
 * A statement is executing within a {@link TransactionContext transaction}
 * and will be able to see all previous changes made within that transaction.
 * When done using a statement it must be closed.
 * 
 * One main difference between a {@link TransactionContext} and a {@link StatementContext}
 * is life cycle of some locks, where read locks can live within one statement,
 * whereas write locks will live for the entire transaction.
 */
public interface StatementContext
{
    /**
     * Returns a label id for a label name. If the label doesn't exist prior to
     * this call it gets created.
     */
    long getOrCreateLabelId( String label ) throws ConstraintViolationKernelException;
    
    /**
     * Returns a label id for a label name. If the label doesn't exist a
     * {@link LabelNotFoundKernelException} will be thrown.
     */
    long getLabelId( String label ) throws LabelNotFoundKernelException;
    
    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link #getOrCreateLabelId(String)} or {@link #getLabelId(String)}.
     */
    boolean addLabelToNode( long labelId, long nodeId );
    
    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     * Label ids are retrieved from {@link #getOrCreateLabelId(String)} or
     * {@link #getLabelId(String)}.
     */
    boolean isLabelSetOnNode( long labelId, long nodeId );
    
    /**
     * Returns all labels set on node with id {@code nodeId}.
     * If the node has no labels an empty {@link Iterable} will be returned.
     */
    Iterable<Long> getLabelsForNode( long nodeId );
    
    /**
     * Returns the label name for the given label id.
     */
    String getLabelName( long labelId ) throws LabelNotFoundKernelException;
    
    /**
     * Closes this statement. Statements must be closed when done and before
     * their parent transaction {@link TransactionContext#finish()} finishes.
     * As an example statement-bound locks can be released when closing
     * a statement. 
     */
    void close( boolean successful );

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link #getOrCreateLabelId(String)} or {@link #getLabelId(String)}.
     */
    boolean removeLabelFromNode( long labelId, long nodeId );

    /**
     * @param labelId the label id of the label that returned nodes are guaranteed to have
     * @return ids of all nodes that have the given label
     */
    Iterable<Long> getNodesWithLabel( long labelId );
    
    /**
     * Adds a {@link IndexRule} to the database which applies globally on both
     * existing as well as new data.
     */
    IndexRule addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException;
    
    /**
     * Returns the index rule ID for the given labelId and propertyKey.
     */
    IndexRule getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException;

    /**
     * Get all indexes for a label.
     */
    Iterable<IndexRule> getIndexRules( long labelId );
    
    /**
     * Returns all index rules.
     */
    Iterable<IndexRule> getIndexRules();

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState getIndexState( IndexRule indexRule ) throws IndexNotFoundKernelException;
    
    /**
     * Drops a {@link IndexRule} from the database
     */
    void dropIndexRule( IndexRule indexRule ) throws ConstraintViolationKernelException;

    /**
     * Returns a property key id for a property key. If the key doesn't exist prior to
     * this call it gets created.
     */
    long getOrCreatePropertyKeyId( String propertyKey );

    /**
     * Returns a property key id for the given property key. If the property key doesn't exist a
     * {@link PropertyKeyNotFoundException} will be thrown.
     */
    long getPropertyKeyId( String propertyKey ) throws PropertyKeyNotFoundException;

    /**
     * Returns the name of a property given it's property key id
     */
    String getPropertyKeyName( long propertyId ) throws PropertyKeyIdNotFoundException;

    /**
     * Returns an iterable with the matched nodes.
     * @throws IndexNotFoundKernelException if no such index found.
     */
    Iterable<Long> exactIndexLookup( long indexId, Object value ) throws IndexNotFoundKernelException;
}
