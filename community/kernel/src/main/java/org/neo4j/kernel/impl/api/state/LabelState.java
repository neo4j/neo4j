/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

/**
 * Represents the transactional changes that involve a particular label:
 * <ul>
 * <li>{@linkplain #nodeDiffSets() Nodes} where the label has been {@linkplain ReadableDiffSets#getAdded() added}
 * or {@linkplain ReadableDiffSets#getRemoved() removed}.</li>
 * <li>{@linkplain #indexChanges() Indexes} for the label that have been
 * {@linkplain ReadableDiffSets#getAdded() created} or {@linkplain ReadableDiffSets#getRemoved() dropped}.</li>
 * <li>{@linkplain #constraintIndexChanges() Unique indexes} for the label that have been
 * {@linkplain ReadableDiffSets#getAdded() created} or {@linkplain ReadableDiffSets#getRemoved() dropped}.</li>
 * <li>{@linkplain #nodeConstraintsChanges() Constraints} for the label that have been
 * {@linkplain ReadableDiffSets#getAdded() created} or {@linkplain ReadableDiffSets#getRemoved() dropped}.</li>
 * </ul>
 */
public abstract class LabelState
{
    public abstract ReadableDiffSets<Long> nodeDiffSets();

    public abstract ReadableDiffSets<IndexDescriptor> indexChanges();

    public abstract ReadableDiffSets<IndexDescriptor> constraintIndexChanges();

    public abstract ReadableDiffSets<NodePropertyConstraint> nodeConstraintsChanges();

    public static class Mutable extends LabelState
    {
        private DiffSets<Long> nodeDiffSets;
        private DiffSets<IndexDescriptor> indexChanges;
        private DiffSets<IndexDescriptor> constraintIndexChanges;
        private DiffSets<NodePropertyConstraint> nodeConstraintsChanges;
        private final int labelId;

        private Mutable( int labelId )
        {
            this.labelId = labelId;
        }

        public int getLabelId()
        {
            return labelId;
        }

        @Override
        public ReadableDiffSets<Long> nodeDiffSets()
        {
            return ReadableDiffSets.Empty.ifNull( nodeDiffSets );
        }

        public DiffSets<Long> getOrCreateNodeDiffSets()
        {
            if ( nodeDiffSets == null )
            {
                nodeDiffSets = new DiffSets<>();
            }
            return nodeDiffSets;
        }

        @Override
        public ReadableDiffSets<IndexDescriptor> indexChanges()
        {
            return ReadableDiffSets.Empty.ifNull( indexChanges );
        }

        public DiffSets<IndexDescriptor> getOrCreateIndexChanges()
        {
            if ( indexChanges == null )
            {
                indexChanges = new DiffSets<>();
            }
            return indexChanges;
        }

        @Override
        public ReadableDiffSets<IndexDescriptor> constraintIndexChanges()
        {
            return ReadableDiffSets.Empty.ifNull( constraintIndexChanges );
        }

        public DiffSets<IndexDescriptor> getOrCreateConstraintIndexChanges()
        {
            if ( constraintIndexChanges == null )
            {
                constraintIndexChanges = new DiffSets<>();
            }
            return constraintIndexChanges;
        }

        @Override
        public ReadableDiffSets<NodePropertyConstraint> nodeConstraintsChanges()
        {
            return ReadableDiffSets.Empty.ifNull( nodeConstraintsChanges );
        }

        public DiffSets<NodePropertyConstraint> getOrCreateConstraintsChanges()
        {
            if ( nodeConstraintsChanges == null )
            {
                nodeConstraintsChanges = new DiffSets<>();
            }
            return nodeConstraintsChanges;
        }
    }

    static abstract class Defaults extends StateDefaults<Integer, LabelState, Mutable>
    {
        @Override
        Mutable createValue( Integer key, TxState state )
        {
            return new Mutable( key );
        }

        @Override
        LabelState defaultValue()
        {
            return DEFAULT;
        }
    }

    private static final LabelState DEFAULT = new LabelState()
    {
        @Override
        public ReadableDiffSets<Long> nodeDiffSets()
        {
            return ReadableDiffSets.Empty.instance();
        }

        @Override
        public ReadableDiffSets<IndexDescriptor> indexChanges()
        {
            return ReadableDiffSets.Empty.instance();
        }

        @Override
        public ReadableDiffSets<IndexDescriptor> constraintIndexChanges()
        {
            return ReadableDiffSets.Empty.instance();
        }

        @Override
        public ReadableDiffSets<NodePropertyConstraint> nodeConstraintsChanges()
        {
            return ReadableDiffSets.Empty.instance();
        }
    };

    private LabelState()
    {
        // limited subclasses
    }
}
