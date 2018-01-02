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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;

public class RelationshipGroupGetter
{
    private final RelationshipGroupStore store;

    public RelationshipGroupGetter( RelationshipGroupStore store )
    {
        this.store = store;
    }

    public RelationshipGroupPosition getRelationshipGroup( NodeRecord node, int type,
            RecordAccess<Long, RelationshipGroupRecord, Integer> relGroupRecords )
    {
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        RecordProxy<Long, RelationshipGroupRecord, Integer> previous = null, current = null;
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            current = relGroupRecords.getOrLoad( groupId, null );
            RelationshipGroupRecord record = current.forReadingData();
            record.setPrev( previousGroupId ); // not persistent so not a "change"
            if ( record.getType() == type )
            {
                return new RelationshipGroupPosition( previous, current );
            }
            else if ( record.getType() > type )
            {   // The groups are sorted in the chain, so if we come too far we can return
                // empty handed right away
                return new RelationshipGroupPosition( previous, null );
            }
            previousGroupId = groupId;
            groupId = record.getNext();
            previous = current;
        }
        return new RelationshipGroupPosition( previous, null );
    }

    public RecordProxy<Long, RelationshipGroupRecord, Integer> getOrCreateRelationshipGroup(
            NodeRecord node, int type, RecordAccess<Long, RelationshipGroupRecord, Integer> relGroupRecords  )
    {
        RelationshipGroupPosition existingGroup = getRelationshipGroup( node, type, relGroupRecords );
        RecordProxy<Long, RelationshipGroupRecord, Integer> change = existingGroup.group();
        if ( change == null )
        {
            assert node.isDense() : "Node " + node + " should have been dense at this point";
            long id = store.nextId();
            change = relGroupRecords.create( id, type );
            RelationshipGroupRecord record = change.forChangingData();
            record.setInUse( true );
            record.setCreated();
            record.setOwningNode( node.getId() );

            // Attach it...
            RecordProxy<Long, RelationshipGroupRecord, Integer> closestPreviousChange = existingGroup.closestPrevious();
            if ( closestPreviousChange != null )
            {   // ...after the closest previous one
                RelationshipGroupRecord closestPrevious = closestPreviousChange.forChangingLinkage();
                record.setNext( closestPrevious.getNext() );
                record.setPrev( closestPrevious.getId() );
                closestPrevious.setNext( id );
            }
            else
            {   // ...first in the chain
                long firstGroupId = node.getNextRel();
                if ( firstGroupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
                {   // There are others, make way for this new group
                    RelationshipGroupRecord previousFirstRecord =
                            relGroupRecords.getOrLoad( firstGroupId, type ).forReadingData();
                    record.setNext( previousFirstRecord.getId() );
                    previousFirstRecord.setPrev( id );
                }
                node.setNextRel( id );
            }
        }
        return change;
    }

    public static class RelationshipGroupPosition
    {
        private final RecordProxy<Long, RelationshipGroupRecord, Integer> closestPrevious, group;

        public RelationshipGroupPosition( RecordProxy<Long, RelationshipGroupRecord, Integer> closestPrevious,
                RecordProxy<Long, RelationshipGroupRecord, Integer> group )
        {
            this.closestPrevious = closestPrevious;
            this.group = group;
        }

        public RecordProxy<Long, RelationshipGroupRecord, Integer> group()
        {
            return group;
        }

        public RecordProxy<Long, RelationshipGroupRecord, Integer> closestPrevious()
        {
            return closestPrevious;
        }
    }
}
