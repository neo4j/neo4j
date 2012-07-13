/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.neo4j.kernel.impl.util.StringLogger;

public class MasterClientResolverTest
{
    @Test
    public void testDefault()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );
        assertEquals( "default was not the latest one", MasterClientResolver.F18.class,
                resolver.getDefault().getClass() );
    }

    @Test
    public void testAskedVersionWorks()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );
        assertEquals( "wrong version returned for version 2", MasterClientResolver.F153.class,
                resolver.getFor( 2, 2 ).getClass() );
        assertEquals( "wrong version returned for version 3", MasterClientResolver.F17.class,
                resolver.getFor( 3, 2 ).getClass() );
        assertEquals( "wrong version returned for version 4", MasterClientResolver.F18.class,
                resolver.getFor( 4, 2 ).getClass() );
    }

    @Test
    public void testUnknownVersionReturnsNull()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );
        assertNull( "Should return nothing on unknwon version", resolver.getFor( -1, -1 ) );
    }

    @Test( expected = NullPointerException.class )
    public void testNonBootstrappedDoesNotReturn()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );
        resolver.instantiate( "", 0, null );
    }

    @Test
    public void testUnknownVersionLeavesPreviousInPlace()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );
        resolver.getDefault();
        MasterClient currentClient = resolver.instantiate( "", 0, null );
        Class previousClass = currentClient.getClass();
        assertNull( resolver.getFor( -1, -1 ) );
        assertEquals( "class was not the same after getting unknown protocol", previousClass, currentClient.getClass() );
        currentClient.shutdown();
    }

    @Test
    public void testAskedVersionSticks()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );

        resolver.getFor( 2, 2 );
        MasterClient currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 2", MasterClient153.class, currentClient.getClass() );
        currentClient.shutdown();

        resolver.getFor( 3, 2 );
        currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 3", MasterClient17.class, currentClient.getClass() );
        currentClient.shutdown();

        resolver.getFor( 4, 2 );
        currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 4", MasterClient18.class, currentClient.getClass() );
        currentClient.shutdown();
    }

    @Test
    public void testDowngradesArePossibleIfNotLocked()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );

        resolver.getFor( 4, 2 );
        MasterClient currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 4", MasterClient18.class, currentClient.getClass() );
        currentClient.shutdown();

        resolver.getFor( 3, 2 );
        currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 4", MasterClient17.class, currentClient.getClass() );
        currentClient.shutdown();

        resolver.getFor( 2, 2 );
        currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 4", MasterClient153.class, currentClient.getClass() );
        currentClient.shutdown();
    }

    @Test
    public void testDowngradesArePossibleUntilLocked()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );

        resolver.getFor( 4, 2 );
        MasterClient currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 4", MasterClient18.class, currentClient.getClass() );
        currentClient.shutdown();

        resolver.getFor( 3, 2 );
        currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 4", MasterClient17.class, currentClient.getClass() );
        currentClient.shutdown();

        resolver.enableDowngradeBarrier();

        resolver.getFor( 2, 2 );
        currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "wrong version returned for version 4", MasterClient17.class, currentClient.getClass() );
        currentClient.shutdown();
    }

    @Test
    public void downgradeDoesNotHappenWhenLocked()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );
        resolver.getFor( 3, 2 );
        resolver.enableDowngradeBarrier();
        resolver.getFor( 2, 2 );
        MasterClient currentClient = resolver.instantiate( "", 0, null );
        assertEquals( "locking should not allow downgrade", MasterClient17.class, currentClient.getClass() );
        currentClient.shutdown();
    }

    @Test
    public void lockingShouldNotPreventBootstrapping()
    {
        MasterClientResolver resolver = new MasterClientResolver( StringLogger.SYSTEM, 0, 0, 0 );
        resolver.enableDowngradeBarrier();

        resolver.getFor( 3, 2 );
        MasterClient currentClient = resolver.instantiate( "", 0, null );
        assertEquals( MasterClient17.class, currentClient.getClass() );
        currentClient.shutdown();

        currentClient = resolver.instantiate( "", 0, null );
        resolver.getFor( 2, 2 );
        assertEquals( "locking should not allow downgrade", MasterClient17.class, currentClient.getClass() );
        currentClient.shutdown();
    }
}
