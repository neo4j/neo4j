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
