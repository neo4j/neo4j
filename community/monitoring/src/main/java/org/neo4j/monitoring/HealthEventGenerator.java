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

public interface HealthEventGenerator {
    HealthEventGenerator NO_OP = new HealthEventGenerator.Adaptor();

    void panic(Throwable causeOfPanic);

    void outOfDiskSpace(Throwable cause);

    class Adaptor implements HealthEventGenerator {
        @Override
        public void panic(Throwable causeOfPanic) {}

        @Override
        public void outOfDiskSpace(Throwable cause) {}
    }
}
