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
package org.neo4j.bolt.protocol.io.reader;

import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.PointValue;

class Point2dReaderTest extends AbstractPointReaderTest {

    @Override
    protected StructReader<?, PointValue> getReader() {
        return Point2dReader.getInstance();
    }

    @Override
    protected double[] getCoordinates() {
        return new double[] {21.0, 42.0};
    }

    @Override
    protected long getStructSize() {
        return 3;
    }
}
