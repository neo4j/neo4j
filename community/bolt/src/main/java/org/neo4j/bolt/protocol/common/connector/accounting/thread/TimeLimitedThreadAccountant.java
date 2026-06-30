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
package org.neo4j.bolt.protocol.common.connector.accounting.thread;

import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.util.Preconditions;

public class TimeLimitedThreadAccountant implements ThreadAccountant {
    private final long maxTimeMillis;
    private final Log logger;

    private final ThreadLocal<Occupation> currentOccupation = new ThreadLocal<>();
    private final Collection<Occupation> occupations = new ConcurrentLinkedQueue<>();

    public TimeLimitedThreadAccountant(long maxTimeMillis, LogService logging) {
        Preconditions.requirePositive(maxTimeMillis);

        this.maxTimeMillis = maxTimeMillis;
        this.logger = logging.getInternalLog(TimeLimitedThreadAccountant.class);
    }

    @Override
    public void execute(String eventName, AccountedRunnable function) throws Exception {
        var startTime = System.currentTimeMillis();

        var occupation = this.currentOccupation.get();
        if (occupation == null) {
            occupation = new Occupation(Thread.currentThread(), eventName, startTime);
            this.currentOccupation.set(occupation);
            this.occupations.add(occupation);
        } else {
            occupation.push(eventName, startTime);
        }

        try {
            function.run();
        } finally {
            if (!occupation.pop()) {
                this.occupations.remove(occupation);
                this.currentOccupation.remove();
            }
        }
    }

    public void reportStuckThreads() {
        var now = System.currentTimeMillis();

        for (Occupation occupation : this.occupations) {
            if (occupation.reported) {
                continue;
            }

            var runTime = occupation.getLatestRuntime(now);
            if (runTime < this.maxTimeMillis) {
                continue;
            }

            var thread = occupation.thread;
            var name = thread.getName();
            var state = thread.getState();
            var stack = thread.getStackTrace();

            var message = (new StringBuilder())
                    .append("Thread \"")
                    .append(name)
                    .append("\" may be stuck!\n")
                    .append("    State: ")
                    .append(state)
                    .append("\n")
                    .append("    Run Time: ")
                    .append(runTime)
                    .append(" ms (maximum configured: ")
                    .append(this.maxTimeMillis)
                    .append(" ms) since last event\n\n")
                    .append("Event Chain:");

            var i = 0;
            for (var event : occupation.events) {
                message.append("\n    #")
                        .append(i++)
                        .append(": ")
                        .append(event.name)
                        .append(" (run time: ")
                        .append(event.getRuntime(now))
                        .append(" ms)");
            }

            message.append("\n\nStack Trace:");

            i = 0;
            for (var e : stack) {
                message.append("\n    ").append("#").append(i++).append(": ").append(e);
            }

            message.append("\n\n").append("This may indicate a bug or severe overload condition.");

            this.logger.warn(message.toString());
            occupation.markReported();
        }
    }

    private static class Occupation {
        private final Thread thread;
        private final BlockingDeque<Event> events = new LinkedBlockingDeque<>();
        private volatile boolean reported;

        public Occupation(Thread thread, String eventName, long startTime) {
            this.thread = thread;
            this.push(eventName, startTime);
        }

        public void push(String eventName, long startTime) {
            this.events.offerLast(new Event(eventName, startTime));
            this.reported = false;
        }

        public boolean pop() {
            var e = this.events.pollLast();

            if (e != null) {
                this.reported = false;
            }

            return !this.events.isEmpty();
        }

        public long getLatestRuntime(long now) {
            var e = this.events.peekLast();
            if (e == null) {
                return 0;
            }

            return e.getRuntime(now);
        }

        public void markReported() {
            this.reported = true;
        }
    }

    private record Event(String name, long startTime) {

        public long getRuntime(long now) {
            return now - this.startTime;
        }
    }
}
