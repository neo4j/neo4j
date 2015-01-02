/**
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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.util.DiffSets;
import org.neo4j.kernel.api.index.IndexDescriptor;

public final class LabelState extends EntityState
{
    private final DiffSets<Long> nodeDiffSets = new DiffSets<Long>();
    private final DiffSets<IndexDescriptor> indexChanges = new DiffSets<IndexDescriptor>();
    private final DiffSets<IndexDescriptor> constraintIndexChanges = new DiffSets<IndexDescriptor>();
    private final DiffSets<UniquenessConstraint> constraintsChanges = new DiffSets<UniquenessConstraint>();

    public LabelState( long id )
    {
        super( id );
    }

    public DiffSets<Long> getNodeDiffSets()
    {
        return nodeDiffSets;
    }

    public DiffSets<IndexDescriptor> indexChanges()
    {
        return indexChanges;
    }

    public DiffSets<IndexDescriptor> constraintIndexChanges()
    {
        return constraintIndexChanges;
    }

    public DiffSets<UniquenessConstraint> constraintsChanges()
    {
        return constraintsChanges;
    }
}
