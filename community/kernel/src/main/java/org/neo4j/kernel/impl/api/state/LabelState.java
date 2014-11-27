/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

public abstract class LabelState
{
    public abstract ReadableDiffSets<Long> nodeDiffSets();

    public abstract ReadableDiffSets<IndexDescriptor> indexChanges();

    public abstract ReadableDiffSets<IndexDescriptor> constraintIndexChanges();

    public abstract ReadableDiffSets<UniquenessConstraint> constraintsChanges();

    public static class Mutable extends LabelState
    {
        private DiffSets<Long> nodeDiffSets = new DiffSets<>();
        private DiffSets<IndexDescriptor> indexChanges = new DiffSets<>();
        private DiffSets<IndexDescriptor> constraintIndexChanges = new DiffSets<>();
        private DiffSets<UniquenessConstraint> constraintsChanges = new DiffSets<>();
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
        public ReadableDiffSets<UniquenessConstraint> constraintsChanges()
        {
            return ReadableDiffSets.Empty.ifNull( constraintsChanges );
        }

        public DiffSets<UniquenessConstraint> getOrCreateConstraintsChanges()
        {
            if ( constraintsChanges == null )
            {
                constraintsChanges = new DiffSets<>();
            }
            return constraintsChanges;
        }
    }

    static abstract class Defaults extends StateDefaults<Integer, LabelState, Mutable>
    {
        @Override
        Mutable createValue( Integer key )
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
        public ReadableDiffSets<UniquenessConstraint> constraintsChanges()
        {
            return ReadableDiffSets.Empty.instance();
        }
    };

    private LabelState()
    {
        // limited subclasses
    }
}
