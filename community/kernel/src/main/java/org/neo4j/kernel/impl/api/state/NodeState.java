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

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.DiffSets;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public final class NodeState extends PropertyContainerState
{
    private DiffSets<Integer> labelDiffSets;
    private RelationshipsAddedToNode relationshipChanges;

    public NodeState( long id )
    {
        super( id );
    }

    public DiffSets<Integer> labelDiffSets()
    {
        if ( null == labelDiffSets )
        {
            labelDiffSets = new DiffSets<>();
        }
        return labelDiffSets;
    }

    public void addRelationship( long relId, int typeId, Direction direction )
    {
        if( !hasRelationshipChanges() )
        {
            relationshipChanges = new RelationshipsAddedToNode();
        }
        relationshipChanges.addRelationship(relId, typeId, direction);
    }

    public PrimitiveLongIterator augmentRelationships( Direction direction, PrimitiveLongIterator rels )
    {
        if(hasRelationshipChanges())
        {
            return relationshipChanges.augmentRelationships( direction, rels );
        }
        return rels;
    }

    public PrimitiveLongIterator augmentRelationships( Direction direction, int[] types, PrimitiveLongIterator rels )
    {
        if(hasRelationshipChanges())
        {
            return relationshipChanges.augmentRelationships( direction, types, rels );
        }
        return rels;
    }

    private boolean hasRelationshipChanges()
    {
        return relationshipChanges != null;
    }
}
