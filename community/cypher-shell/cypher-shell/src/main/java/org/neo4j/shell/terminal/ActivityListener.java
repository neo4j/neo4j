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
package org.neo4j.shell.terminal;

import java.util.Objects;

/**
 * One-shot wiring point between {@link ActivityTrackingTerminal} (created first, needs a listener right away)
 * and {@link JlineTerminal} (created afterwards, owns the idle-timeout services the listener should notify).
 */
class ActivityListener {
    private volatile Runnable onActivity = () -> {};

    void set(Runnable onActivity) {
        this.onActivity = Objects.requireNonNull(onActivity);
    }

    void fire() {
        onActivity.run();
    }
}
