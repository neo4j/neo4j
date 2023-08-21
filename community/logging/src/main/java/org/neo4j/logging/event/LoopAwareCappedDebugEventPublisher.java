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
package org.neo4j.logging.event;

import java.util.ArrayList;
import java.util.List;

/**
 * For the first loop, everything is logged.
 * For the next loop, if all the log events are the same, nothing is logged.
 * If any log event is different, all of them are logged, including the ones in the past.
 * That loop is now the new baseline for future loops.
 * If we reach the maximum number of loops without any change, publish an event that says so, publish that loop of
 * events, and reset the counter.
 *
 * <b>N.B.</b> If you use this class without calling {@link #loopComplete()} it will continue to accumulate events
 * and consume more memory, potentially leading to running out of heap. So consider the amount of memory used by the
 * events of each loop when deciding to use this class.
 */
public class LoopAwareCappedDebugEventPublisher implements LoopAwareDebugEventPublisher {

    private final DebugEventPublisher delegate;
    private final int maximumNumberOfLoopsWithoutPassingOnEvent;

    private List<Event> lastLoggedLoop = null;
    private List<Event> currentLoop = new ArrayList<>();
    private int loopsWithNoChange = 0;

    public LoopAwareCappedDebugEventPublisher(
            DebugEventPublisher delegate, int maximumNumberOfLoopsWithoutPassingOnEvent) {
        this.delegate = delegate;
        this.maximumNumberOfLoopsWithoutPassingOnEvent = maximumNumberOfLoopsWithoutPassingOnEvent;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private synchronized void addEvent(Event event) {
        currentLoop.add(event);
        if (lastLoggedLoop == null) {
            event.apply(delegate);
        } else if (lastLoggedLoop.size() >= currentLoop.size()
                && lastLoggedLoop.get(currentLoop.size() - 1).equals(event)) {
            // Same so far
        } else {
            // Different now
            lastLoggedLoop = null;
            logWholeLoop();
        }
    }

    @Override
    public synchronized void loopComplete() {
        if (lastLoggedLoop == null) {
            // First loop, or found a different message part way through the loop
            lastLoggedLoop = currentLoop;
        } else if (!lastLoggedLoop.equals(currentLoop)) {
            // Loops had different content at the end (extra or missing events)
            logWholeLoop();
            lastLoggedLoop = currentLoop;
        } else {
            // Loop was the same
            if (loopsWithNoChange >= maximumNumberOfLoopsWithoutPassingOnEvent) {
                logWholeLoop();
            } else {
                loopsWithNoChange++;
            }
        }
        currentLoop = new ArrayList<>();
    }

    private void logWholeLoop() {
        if (loopsWithNoChange > 0) {
            delegate.publish(
                    Type.Info,
                    String.format(
                            "There were %d repetitions of the same loop of events which were not logged",
                            loopsWithNoChange));
            loopsWithNoChange = 0;
        }
        currentLoop.forEach(storedLogEvent -> storedLogEvent.apply(delegate));
    }

    @Override
    public void publish(Type type, String message, Parameters parameters) {
        addEvent(new Event(type, message, parameters));
    }

    @Override
    public void publish(Type type, String message) {
        addEvent(new Event(type, message, null));
    }

    private record Event(Type type, String message, Parameters parameters) {
        void apply(DebugEventPublisher publisher) {
            if (parameters == null) {
                publisher.publish(type, message);
            } else {
                publisher.publish(type, message, parameters);
            }
        }
    }
}
