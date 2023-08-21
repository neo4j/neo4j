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
package org.neo4j.monitoring;

import java.util.Objects;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;

public class DatabaseHealth extends LifecycleAdapter implements Panic, OutOfDiskSpace {
    private static final String panicMessage = "The database has encountered a critical error, "
            + "and needs to be restarted. Please see database logs for more details.";
    public static final String outOfDiskSpaceMessage = "The database was unable to allocate enough disk space.";

    private volatile boolean hasPanic;
    private final HealthEventGenerator healthEventGenerator;
    private final InternalLog log;
    private volatile Throwable causeOfPanic;

    public DatabaseHealth(HealthEventGenerator healthEventGenerator, InternalLog log) {
        this.healthEventGenerator = healthEventGenerator;
        this.log = log;
    }

    /**
     * Asserts that the database is in good health. If that is not the case then the cause of the
     * unhealthy state is wrapped in an exception of the given type, i.e. the panic disguise.
     *
     * @param panicDisguise the cause of the unhealthy state wrapped in an exception of this type.
     * @throws EXCEPTION exception type to wrap cause in.
     */
    @Override
    public <EXCEPTION extends Throwable> void assertNoPanic(Class<EXCEPTION> panicDisguise) throws EXCEPTION {
        if (hasPanic) {
            throw Exceptions.disguiseException(panicDisguise, panicMessage, causeOfPanic);
        }
    }

    @Override
    public synchronized void panic(Throwable cause) {
        if (hasPanic) {
            return;
        }

        Objects.requireNonNull(cause, "Must provide a non null cause for the database panic");
        this.causeOfPanic = cause;
        this.hasPanic = true;
        log.error("Database panic: " + panicMessage, cause);
        healthEventGenerator.panic(cause);
    }

    @Override
    public boolean hasNoPanic() {
        return !hasPanic;
    }

    @Override
    public Throwable causeOfPanic() {
        return causeOfPanic;
    }

    @Override
    public void outOfDiskSpace(Throwable cause) {
        log.error("Database out of disk space: " + outOfDiskSpaceMessage, cause);
        healthEventGenerator.outOfDiskSpace(cause);
    }
}
