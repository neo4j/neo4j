package org.neo4j.server.rest.web.paging;

import org.neo4j.server.database.AbstractInjectableProvider;

import com.sun.jersey.api.core.HttpContext;

public class RealClockProvider extends AbstractInjectableProvider<RealClock>
{
    public RealClockProvider()
    {
        super(RealClock.class);
    }

    @Override
    public RealClock getValue( HttpContext arg0 )
    {
        return new RealClock();
    }

}
