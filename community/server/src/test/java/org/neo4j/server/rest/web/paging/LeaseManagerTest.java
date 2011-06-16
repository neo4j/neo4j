package org.neo4j.server.rest.web.paging;

import static org.junit.Assert.*;

import org.junit.Test;

public class LeaseManagerTest
{
    private static final long ONE_MINUTE = 60;

    @Test
    public void shouldCreateADefaultLeaseForAPagedTraverser()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<Object> manager = new LeaseManager<Object>( fakeClock );

        assertNotNull( manager.createLease() );
    }

    @Test
    public void shouldRetrieveAnExistingLeaseImmediatelyAfterCreation()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<Object> manager = new LeaseManager<Object>( fakeClock );

        Lease<Object> lease = manager.createLease();

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldRetrieveAnExistingLeaseSomeTimeAfterCreation()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<Object> manager = new LeaseManager<Object>( fakeClock );

        Lease<Object> lease = manager.createLease( 2 * 60 );
        fakeClock.forwardMinutes( 1 );

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldNotRetrieveALeaseAfterItExpired()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<Object> manager = new LeaseManager<Object>( fakeClock );

        Lease<Object> lease = manager.createLease( ONE_MINUTE );

        fakeClock.forwardMinutes( 2 );

        assertNull( manager.getLeaseById( lease.getId() ) );
    }

    @Test
    public void shouldNotBarfWhenAnotherThreadOrRetrieveRevokesTheLease()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<Object> manager = new LeaseManager<Object>( fakeClock );

        Lease<Object> leaseA = manager.createLease( ONE_MINUTE );
        Lease<Object> leaseB = manager.createLease( ONE_MINUTE * 3 );

        fakeClock.forwardMinutes( 2 );

        assertNotNull( manager.getLeaseById( leaseB.getId() ) );
        assertNull( manager.getLeaseById( leaseA.getId() ) );

    }
}
