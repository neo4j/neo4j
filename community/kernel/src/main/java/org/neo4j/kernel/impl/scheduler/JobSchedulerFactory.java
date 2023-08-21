/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.scheduler;

import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

public final class JobSchedulerFactory {
    private JobSchedulerFactory() {}

    public static JobScheduler createScheduler() {
        return createCentralScheduler(Clocks.nanoClock(), NullLogProvider.getInstance());
    }

    public static JobScheduler createInitialisedScheduler() {
        return createInitialisedScheduler(Clocks.nanoClock());
    }

    public static JobScheduler createInitialisedScheduler(SystemNanoClock clock) {
        return createInitialisedScheduler(clock, NullLogProvider.getInstance());
    }

    public static JobScheduler createInitialisedScheduler(SystemNanoClock clock, InternalLogProvider logProvider) {
        CentralJobScheduler scheduler = createCentralScheduler(clock, logProvider);
        scheduler.init();
        return scheduler;
    }

    private static CentralJobScheduler createCentralScheduler(SystemNanoClock clock, InternalLogProvider logProvider) {
        return new CentralJobScheduler(clock, logProvider);
    }
}
