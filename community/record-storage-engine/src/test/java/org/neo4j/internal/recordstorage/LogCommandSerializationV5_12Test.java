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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
public class LogCommandSerializationV5_12Test extends LogCommandSerializationV5_11Test {

    @Override
    LogCommandSerializationV5_12 createReader() {
        return LogCommandSerializationV5_12.INSTANCE;
    }

    @Override
    LogCommandSerializationV5_12 writer() {
        return LogCommandSerializationV5_12.INSTANCE;
    }

    @Override
    byte[] enrichmentUserMetadata() {
        return random.nextBytes(new byte[random.nextInt(0, 123)]);
    }
}
