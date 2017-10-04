/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;

/**
 * Represents the transactional changes that involve a particular label:
 * <ul>
 * <li>{@linkplain #nodeDiffSets() Nodes} where the label has been {@linkplain ReadableDiffSets#getAdded() added}
 * or {@linkplain ReadableDiffSets#getRemoved() removed}.</li>
 * </ul>
 */
public abstract class LabelState
{
    public abstract ReadableDiffSets<Long> nodeDiffSets();

    public static class Mutable extends LabelState
    {
        private DiffSets<Long> nodeDiffSets;
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
    }

    abstract static class Defaults extends StateDefaults<Integer, LabelState, Mutable>
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
    };

    private LabelState()
    {
        // limited subclasses
    }
}
