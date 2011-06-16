package org.neo4j.server.rest.web.paging;

public class FakeClock implements Clock
{
    private long time = System.currentTimeMillis();
    
    @Override
    public long currentTimeInMilliseconds()
    {
        return time;
    }

    public void forwardMinutes( int minutes )
    {
        time += 60000 * minutes;
    }
}
