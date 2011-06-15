package org.neo4j.server.rest.web.paging;

public class RealClock implements Clock
{

    @Override
    public long currentTimeInMilliseconds()
    {
        return System.currentTimeMillis();
    }

}
