/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.neo4j.examples.socnet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.helpers.collection.PositionedIterator;

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
