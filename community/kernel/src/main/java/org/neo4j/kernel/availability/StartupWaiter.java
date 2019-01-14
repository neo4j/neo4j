/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.availability;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * At end of startup, wait for instance to become available for transactions.
 * <p>
 * This helps users who expect to be able to access the instance after
 * the constructor is run.
 */
public class StartupWaiter extends LifecycleAdapter
{
    private final AvailabilityGuard availabilityGuard;
    private final long timeout;

    public StartupWaiter( AvailabilityGuard availabilityGuard, long timeout )
    {
        this.availabilityGuard = availabilityGuard;
        this.timeout = timeout;
    }

    @Override
    public void start()
    {
        availabilityGuard.isAvailable( timeout );
    }
}
