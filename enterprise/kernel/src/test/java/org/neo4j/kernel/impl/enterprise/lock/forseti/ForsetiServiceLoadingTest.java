/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
