package org.neo4j.kernel.ha.lock.forseti;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;

import static org.junit.Assert.assertEquals;

/** Test output of various Forseti introspection mechanisms */
public class ForsetiMessagesTest
{
    @Rule public OtherThreadRule otherThread = new OtherThreadRule<>();

    @Test
    public void shouldIncludeFriendlyNameInDeadlockExceptions() throws Throwable
    {
        // Given
        ForsetiLockManager locks = new ForsetiLockManager( ResourceTypes.NODE );

        final Locks.Client client1 = locks.newClient().description( "`MATCH (n) RETURN n`" );
        final Locks.Client client2 = locks.newClient().description( "`MATCH (p) SET p.name = {name}` {name:'Bob'}" );
        final Locks.Client client3 = locks.newClient().description( "`MERGE (n) RETURN id(n)`" );

        client1.acquireShared( ResourceTypes.NODE, 1337 );
        client3.acquireShared( ResourceTypes.NODE, 1337 );
        client2.acquireExclusive( ResourceTypes.NODE, 1338 );
        Future future = otherThread.execute( new OtherThreadExecutor.WorkerCommand<Void,Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                client1.acquireExclusive( ResourceTypes.NODE, 1338 );
                return null;
            }
        } );

        String client2Error = null, client1Error = null;

        // When
        try
        {
            client2.acquireExclusive( ResourceTypes.NODE, 1337 );
        }
        catch( DeadlockDetectedException e )
        {
            client2Error = e.getMessage();
        }

        // Or when
        try
        {
            future.get();
        }
        catch( ExecutionException e )
        {
            client1Error = e.getCause().getMessage();
        }

        // Then
        if( client1Error != null )
        {
            assertEquals("Tx[0] can't lock NODE(1338), because that resource is locked by others in a way that would cause a deadlock if we waited for them" +
                         ".\n" +
                         "The lock currently is ExclusiveLock{owner=Tx[1]}, and holders of that lock are waiting in the following way: \n" +
                         "<Tx[1], waiting for Tx[0],Tx[2]>\n" +
                         "\n" +
                         "Transactions:\n" +
                         "  Tx[0]: `MATCH (n) RETURN n`\n" +
                         "  Tx[1]: `MATCH (p) SET p.name = {name}` {name:'Bob'}\n" +
                         "  Tx[2]: `MERGE (n) RETURN id(n)`", client1Error);
        }
        else
        {
            assertEquals("Tx[1] can't lock NODE(1337), because that resource is locked by others in a way that would cause a deadlock if we waited for them" +
                         ".\n" +
                         "The lock currently is SharedLock{objectId=233530418, refCount=2}, and holders of that lock are waiting in the following way: \n" +
                         "<Tx[0], waiting for Tx[1],Tx[2]>, \n" +
                         "<Tx[2], running>\n" +
                         "\n" +
                         "Transactions:\n" +
                         "  Tx[0]: `MATCH (n) RETURN n`\n" +
                         "  Tx[1]: `MATCH (p) SET p.name = {name}` {name:'Bob'}\n" +
                         "  Tx[2]: `MERGE (n) RETURN id(n)`", client2Error);
        }
    }
}