/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ForsetiServiceLoadingTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();

    @Test
    public void shouldUseForsetiAsDefaultLockManager()
    {
        // When
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // Then
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( ForsetiLockManager.class ) );
    }

    @Test
    public void shouldAllowUsingCommunityLockManager()
    {
        // When
        dbRule.setConfig( GraphDatabaseFacadeFactory.Configuration.lock_manager, "community" );
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // Then
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( CommunityLockManger.class ) );
    }
}
