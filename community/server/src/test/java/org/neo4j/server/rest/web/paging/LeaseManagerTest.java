package org.neo4j.server.rest.web.paging;

import static org.junit.Assert.*;

import org.junit.Test;

public class LeaseManagerTest
{
    @Test
    public void shouldCreateADefaultLeaseForAPagedTraverser() {
        FakeClock fakeClock = new FakeClock();
        LeaseManager manager = new LeaseManager(new FakeClock());
        
        assertNotNull(manager.createLease());
    }
    
    @Test
    public void shouldRetrieveAnExistingLease() {
        FakeClock fakeClock = new FakeClock();
        LeaseManager manager = new LeaseManager(new FakeClock());
        
        Lease lease = manager.createLease();
        
        manager.getLeaseById(lease.getId());

    }
}
