/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */



package org.neo4j.examples.socnet;

import org.neo4j.helpers.collection.PositionedIterator;

import java.util.*;

class FriendsStatusUpdateIterator implements Iterator<StatusUpdate> {
    private ArrayList<PositionedIterator<StatusUpdate>> statuses = new ArrayList<PositionedIterator<StatusUpdate>>();
    private StatusUpdateComparator comparator = new StatusUpdateComparator();

    public FriendsStatusUpdateIterator( Person person )
    {
        for ( Person friend : person.getFriends() )
        {
            Iterator<StatusUpdate> iterator = friend.getStatus().iterator();
            if (iterator.hasNext()) {
                statuses.add(new PositionedIterator<StatusUpdate>(iterator));
            }
        }

        sort();
    }

    public boolean hasNext()
    {
        return statuses.size() > 0;
    }

    public StatusUpdate next()
    {
        if ( statuses.size() == 0 )
        {
            throw new NoSuchElementException();
        }
        // START SNIPPET: getActivityStream
        PositionedIterator<StatusUpdate> first = statuses.get(0);
        StatusUpdate returnVal = first.current();

        if ( !first.hasNext() )
        {
            statuses.remove( 0 );
        }
        else
        {
            first.next();
            sort();
        }

        return returnVal;
        // END SNIPPET: getActivityStream
    }

    private void sort()
    {
        Collections.sort( statuses, comparator );
    }

    public void remove()
    {
        throw new UnsupportedOperationException( "Don't know how to do that..." );
    }

    private class StatusUpdateComparator implements Comparator<PositionedIterator<StatusUpdate>> {
        public int compare(PositionedIterator<StatusUpdate> a, PositionedIterator<StatusUpdate> b) {
            return a.current().getDate().compareTo(b.current().getDate());
        }
    }
}
