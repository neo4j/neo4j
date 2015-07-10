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
package org.neo4j.kernel.ha.lock.forseti;

import org.junit.Test;

import org.neo4j.embedded.CommunityTestGraphDatabase;
import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ForsetiServiceLoadingTest
{
    @Test
    public void shouldUseForsetiAsDefaultLockManager() throws Exception
    {
        // When
        TestGraphDatabase db = CommunityTestGraphDatabase.openEphemeral();

        // Then
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( ForsetiLockManager.class ) );
    }

    @Test
    public void shouldAllowUsingCommunityLockManager() throws Exception
    {
        // When
        TestGraphDatabase db = CommunityTestGraphDatabase.buildEphemeral()
                .withSetting( GraphDatabaseFacadeFactory.Configuration.lock_manager, "community" )
                .open();

        // Then
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( CommunityLockManger.class ) );
    }
}
