/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.locking.forseti;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.locking.LockingCompatibilityTestSuite;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.time.SystemNanoClock;

public class ForsetiLocksTest extends LockingCompatibilityTestSuite {
    @Override
    protected Locks createLockManager(Config config, SystemNanoClock clock) {
        return new ForsetiLockManager(config, clock, ResourceTypes.values());
    }

    @Override
    protected boolean isAwaitingLockAcquisition(Actor actor) throws Exception {
        actor.untilWaitingIn(ForsetiClient.class.getDeclaredMethod(
                "waitFor", ForsetiLockManager.Lock.class, ResourceType.class, long.class, int.class));
        return true;
    }
}
