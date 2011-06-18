package org.neo4j.server.rest.web.paging;

import org.neo4j.server.database.AbstractInjectableProvider;

import com.sun.jersey.api.core.HttpContext;

public class RealClockProvider extends AbstractInjectableProvider<Clock>
{
    public RealClockProvider()
    {
        super(Clock.class);
    }

    @Override
    public Clock getValue( HttpContext arg0 )
    {
        return new RealClock();
    }
}
