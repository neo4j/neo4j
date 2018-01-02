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

import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;

public class RelationshipGroupGetter
{
    private final IdSequence idGenerator;

    public RelationshipGroupGetter( IdSequence idGenerator )
    {
        this.idGenerator = idGenerator;
    }

    public RelationshipGroupPosition getRelationshipGroup( NodeRecord node, int type,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords )
    {
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        RecordProxy<RelationshipGroupRecord, Integer> previous = null;
        RecordProxy<RelationshipGroupRecord, Integer> current;
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

    public RecordProxy<RelationshipGroupRecord, Integer> getOrCreateRelationshipGroup(
            NodeRecord node, int type, RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords  )
    {
        RelationshipGroupPosition existingGroup = getRelationshipGroup( node, type, relGroupRecords );
        RecordProxy<RelationshipGroupRecord, Integer> change = existingGroup.group();
        if ( change == null )
        {
            assert node.isDense() : "Node " + node + " should have been dense at this point";
            long id = idGenerator.nextId();
            change = relGroupRecords.create( id, type );
            RelationshipGroupRecord record = change.forChangingData();
            record.setInUse( true );
            record.setCreated();
            record.setOwningNode( node.getId() );

            // Attach it...
            RecordProxy<RelationshipGroupRecord, Integer> closestPreviousChange = existingGroup.closestPrevious();
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
        private final RecordProxy<RelationshipGroupRecord, Integer> closestPrevious;
        private final RecordProxy<RelationshipGroupRecord, Integer> group;

        public RelationshipGroupPosition( RecordProxy<RelationshipGroupRecord, Integer> closestPrevious,
                RecordProxy<RelationshipGroupRecord, Integer> group )
        {
            this.closestPrevious = closestPrevious;
            this.group = group;
        }

        public RecordProxy<RelationshipGroupRecord, Integer> group()
        {
            return group;
        }

        public RecordProxy<RelationshipGroupRecord, Integer> closestPrevious()
        {
            return closestPrevious;
        }
    }
}
