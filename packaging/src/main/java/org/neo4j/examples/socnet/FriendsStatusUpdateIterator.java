package org.neo4j.examples.socnet;

import java.util.*;

/**
* Created by IntelliJ IDEA.
* User: ata
* Date: Sep 13, 2010
* Time: 10:21:52 AM
* To change this template use File | Settings | File Templates.
*/
class FriendsStatusUpdateIterator implements Iterator<StatusUpdate> {
    private ArrayList<SingleStatusUpdateIterator> statuses = new ArrayList<SingleStatusUpdateIterator>();
    private StatusUpdateComparator comparator = new StatusUpdateComparator();

    public FriendsStatusUpdateIterator(Person person) {
        for (Person friend : person.getFriends()) {
            Iterator<StatusUpdate> iterator = friend.getStatus().iterator();
            if (iterator.hasNext()) {
                statuses.add(new SingleStatusUpdateIterator(iterator));
            }
        }

        sort();
    }

    public boolean hasNext() {
        return statuses.size() > 0;
    }

    public StatusUpdate next() {
        if(statuses.size() == 0)
            throw new NoSuchElementException();

        SingleStatusUpdateIterator first = statuses.get(0);
        StatusUpdate returnVal = first.current();

        if(!first.hasNext()) {
            statuses.remove(0);
        } else {
            first.next();
            sort();
        }


        return returnVal;
    }

    private void sort() {
        Collections.sort(statuses, comparator);
    }

    public void remove() {
        throw new UnsupportedOperationException("Don't know how to do that...");
    }


    private class StatusUpdateComparator implements Comparator<SingleStatusUpdateIterator> {
        public int compare(SingleStatusUpdateIterator a, SingleStatusUpdateIterator b) {
            return a.current().getDate().compareTo(b.current().getDate());
        }
    }
}
