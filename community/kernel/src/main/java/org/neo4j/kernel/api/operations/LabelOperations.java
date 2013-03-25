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
package org.neo4j.kernel.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;

public interface LabelOperations
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
     * Returns the label name for the given label id.
     */
    String getLabelName( long labelId ) throws LabelNotFoundKernelException;

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
    Iterator<Long> getLabelsForNode( long nodeId );

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link #getOrCreateLabelId(String)} or {@link #getLabelId(String)}.
     */
    boolean removeLabelFromNode( long labelId, long nodeId );

}
