/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.test.OtherThreadRule.isThreadState;

/** Test output of various Forseti introspection mechanisms */
public class ForsetiMessagesTest
{
    @Rule public OtherThreadRule otherThread = new OtherThreadRule<>();

    @Test
    public void shouldIncludeFriendlyNameInDeadlockExceptions() throws Throwable
    {
        // Given
        ForsetiLockManager locks = new ForsetiLockManager( ResourceTypes.NODE );

        final Locks.Client tx0 = locks.newClient().description( "`MATCH (n) RETURN n`" );
        final Locks.Client tx1 = locks.newClient().description( "`MATCH (p) SET p.name = {name}` {name:'Bob'}" );
        final Locks.Client tx2 = locks.newClient().description( "`MERGE (n) RETURN id(n)`" );

        tx0.acquireShared( ResourceTypes.NODE, 1337 );
        tx2.acquireShared( ResourceTypes.NODE, 1337 );
        tx1.acquireExclusive( ResourceTypes.NODE, 1338 );
        Future future = otherThread.execute( new OtherThreadExecutor.WorkerCommand<Void,Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                tx0.acquireExclusive( ResourceTypes.NODE, 1338 ); // blocked here
                return null;
            }
        } );
        assertThat( otherThread, isThreadState( Thread.State.TERMINATED, Thread.State.TIMED_WAITING, Thread.State.WAITING ));

        String tx1Error = null, tx0Error = null;

        // When
        try
        {
            tx1.acquireExclusive( ResourceTypes.NODE, 1337 );
        }
        catch( DeadlockDetectedException e )
        {
            tx1Error = e.getMessage();
            tx1.releaseAll();
        }

        // Or when
        try
        {
            future.get(); // blocked here
        }
        catch( ExecutionException e )
        {
            tx0Error = e.getCause().getMessage();
        }

        // Then
        if( tx0Error != null )
        {
            assertEquals("Tx[0] can't lock NODE(1338), because that resource is locked by others in a way that would cause a deadlock if we waited for them" +
                         ".\n" +
                         "The lock currently is ExclusiveLock{owner=Tx[1]}, and holders of that lock are waiting in the following way: \n" +
                         "<Tx[1], waiting for Tx[0],Tx[2]>\n" +
                         "\n" +
                         "Transactions:\n" +
                         "  Tx[0]: `MATCH (n) RETURN n`\n" +
                         "  Tx[1]: `MATCH (p) SET p.name = {name}` {name:'Bob'}\n" +
                         "  Tx[2]: `MERGE (n) RETURN id(n)`", tx0Error);
        }
        else
        {
            // object id in SharedLock is non-deterministic, and we'd like to avoid a massive regex, so just replace the object id before checking equality
            assertThat( tx1Error, matchesPattern("^" +
                         "Tx\\[1\\] can't lock NODE\\(1337\\), because that resource is locked by others in a way that would cause a deadlock if we waited for them" +
                         ".\n" +
                         "The lock currently is SharedLock\\{objectId=\\d+, refCount=2\\}, and holders of that lock are waiting in the following way: \n" +
                         "<Tx\\[0\\], waiting for .+>, \n" +
                         "<Tx\\[2\\], running>\n" +
                         "\n" +
                         "Transactions:\n" +
                         "  Tx\\[0\\]: `MATCH \\(n\\) RETURN n`\n" +
                         "  Tx\\[1\\]: `MATCH \\(p\\) SET p.name = \\{name\\}` \\{name:'Bob'\\}\n" +
                         "  Tx\\[2\\]: `MERGE \\(n\\) RETURN id\\(n\\)`"));
        }
    }

    public static Matcher<String> matchesPattern( final String pattern )
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            protected boolean matchesSafely( String item )
            {
                return item.matches( pattern );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Matches[" ).appendText( pattern ).appendText( "]" );
            }
        };
    }
}
