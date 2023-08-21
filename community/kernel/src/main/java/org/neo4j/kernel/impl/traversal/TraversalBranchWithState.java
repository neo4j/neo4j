/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;
import org.neo4j.internal.helpers.collection.ResourceClosingIterator;

public class TraversalBranchWithState extends TraversalBranchImpl implements BranchState {
    protected final Object stateForMe;
    protected Object stateForChildren;

    public TraversalBranchWithState(
            TraversalBranch parent, int depth, Node source, Relationship toHere, Object inheritedState) {
        super(parent, depth, source, toHere);
        this.stateForMe = this.stateForChildren = inheritedState;
    }

    public TraversalBranchWithState(TraversalBranch parent, Node source, InitialBranchState initialState) {
        super(parent, source);
        this.stateForMe = this.stateForChildren = initialState.initialState(this);
    }

    @Override
    public void setState(Object state) {
        this.stateForChildren = state;
    }

    @Override
    public Object getState() {
        return this.stateForMe;
    }

    @Override
    protected TraversalBranch newNextBranch(Node node, Relationship relationship) {
        return new TraversalBranchWithState(this, length() + 1, node, relationship, stateForChildren);
    }

    @Override
    protected ResourceIterator<Relationship> expandRelationshipsWithoutChecks(PathExpander expander) {
        return ResourceClosingIterator.fromResourceIterable(expander.expand(this, this));
    }

    @Override
    public Object state() {
        return this.stateForMe;
    }

    @Override
    protected void evaluate(TraversalContext context) {
        setEvaluation(context.evaluate(this, this));
    }
}
