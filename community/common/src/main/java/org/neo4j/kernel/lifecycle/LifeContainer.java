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
package org.neo4j.kernel.lifecycle;

/**
 * A {@link Lifecycle} that has a {@link LifeSupport} inside of it. Convenient in situations where you have a component involving
 * multiple {@link Lifecycle} "child" instances.
 */
public class LifeContainer implements Lifecycle {
    protected final LifeSupport life = new LifeSupport();

    @Override
    public void init() {
        life.init();
    }

    @Override
    public void start() {
        life.start();
    }

    @Override
    public void stop() {
        life.stop();
    }

    @Override
    public void shutdown() {
        life.shutdown();
    }
}
