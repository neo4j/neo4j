package org.neo4j.server.rest.web.paging;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class LeaseManagerTest
{
    private static final long ONE_MINUTE = 60;

    private static class LeasableObject implements Leasable
    {
    }

    @Test
    public void shouldCreateADefaultLease()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );
        assertNotNull( manager.createLease( new LeasableObject() ) );
    }

    @Test
    public void shouldNotAcceptLeasesWithNegativeTTL()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );
        assertNull( manager.createLease( -1l, new LeasableObject() ) );
        assertNull( manager.createLease( Long.MAX_VALUE + 1, new LeasableObject() ) );
    }

    @Test
    public void shouldRetrieveAnExistingLeaseImmediatelyAfterCreation()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> lease = manager.createLease( new LeasableObject() );

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldRetrieveAnExistingLeaseSomeTimeAfterCreation()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> lease = manager.createLease( 2 * 60, new LeasableObject() );
        fakeClock.forwardMinutes( 1 );

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldNotRetrieveALeaseAfterItExpired()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> lease = manager.createLease( ONE_MINUTE, new LeasableObject() );

        fakeClock.forwardMinutes( 2 );

        assertNull( manager.getLeaseById( lease.getId() ) );
    }

    @Test
    public void shouldNotBarfWhenAnotherThreadOrRetrieveRevokesTheLease()
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> leaseA = manager.createLease( ONE_MINUTE, new LeasableObject() );
        Lease<LeasableObject> leaseB = manager.createLease( ONE_MINUTE * 3, new LeasableObject() );

        fakeClock.forwardMinutes( 2 );

        assertNotNull( manager.getLeaseById( leaseB.getId() ) );
        assertNull( manager.getLeaseById( leaseA.getId() ) );

    }
}
