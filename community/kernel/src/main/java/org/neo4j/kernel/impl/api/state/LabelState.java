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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class LabelState extends EntityState
{

    public static final StateFactory<LabelState> FACTORY = new StateFactory<LabelState>()
    {
        @Override
        public LabelState newState( long id )
        {
            return new LabelState(id);
        }
    };

    private final DiffSets<Long> nodeDiffSets = new DiffSets<Long>();
    private final DiffSets<IndexRule> indexRuleDiffSets = new DiffSets<IndexRule>();

    public LabelState( long id )
    {
        super( id );
    }

    public DiffSets<Long> getNodeDiffSets()
    {
        return nodeDiffSets;
    }

    public DiffSets<IndexRule> getIndexRuleDiffSets()
    {
        return indexRuleDiffSets;
    }
}
