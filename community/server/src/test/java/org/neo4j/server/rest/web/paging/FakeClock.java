package org.neo4j.server.rest.web.paging;

public class FakeClock implements Clock
{
    private long time = System.currentTimeMillis();
    
    @Override
    public long currentTimeInMilliseconds()
    {
        return time;
    }
}
