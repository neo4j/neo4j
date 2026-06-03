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
package org.neo4j.fleetmanagement.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.beans.PropertyChangeListener;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StateTest {

    @Test
    void shouldRemoveAllListeners() {
        State state = new State();
        AtomicInteger callCount = new AtomicInteger(0);
        PropertyChangeListener listener = evt -> callCount.incrementAndGet();

        state.addPropertyChangeListener(listener);
        state.setActive(true);
        assertThat(callCount.get()).isOne();

        state.removePropertyChangeListeners();
        state.setActive(false);
        assertThat(callCount.get())
                .as("Listener should not have been called after removal")
                .isOne();
    }
}
