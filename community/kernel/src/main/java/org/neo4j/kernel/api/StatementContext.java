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
     * 
     * @param label the name of the label to get the id for.
     * @return the label id for the given label name.
     * @throws ConstraintViolationKernelException if the label name violates some
     * constraint, for example if it's null or of zero length.
     */
    long getOrCreateLabelId( String label ) throws ConstraintViolationKernelException;
    
    /**
     * Returns a label if for a label name. If the label doesn't exist a
     * {@link LabelNotFoundKernelException} will be thrown.
     * 
     * @param label the name of the label to get the id for.
     * @return the label id for the given label name.
     * @throws LabelNotFoundKernelException if the label for the given name
     * doesn't exist.
     */
    long getLabelId( String label ) throws LabelNotFoundKernelException;
    
    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link #getOrCreateLabelId(String)} or {@link #getLabelId(String)}.
     * 
     * @param labelId the label id to label the node with.
     * @param nodeId the node id to add the label to.
     */
    void addLabelToNode( long labelId, long nodeId );
    
    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     * Label ids are retrieved from {@link #getOrCreateLabelId(String)} or
     * {@link #getLabelId(String)}.
     * 
     * @param labelId the label to check.
     * @param nodeId the node to check.
     * @return whether or not the node is labeled with the given label.
     */
    boolean isLabelSetOnNode( long labelId, long nodeId );
    
    /**
     * Closes this statement. Statements must be closed when done and before
     * their parent transaction {@link TransactionContext#finish()} finishes.
     * As an example statement-bound locks can be released when closing
     * a statement. 
     */
    void close();
}
