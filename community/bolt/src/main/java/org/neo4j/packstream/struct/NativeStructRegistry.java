/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.packstream.struct;

import org.neo4j.packstream.struct.value.DateReader;
import org.neo4j.packstream.struct.value.DateTimeReader;
import org.neo4j.packstream.struct.value.DateTimeZoneIdReader;
import org.neo4j.packstream.struct.value.DurationReader;
import org.neo4j.packstream.struct.value.LocalDateTimeReader;
import org.neo4j.packstream.struct.value.LocalTimeReader;
import org.neo4j.packstream.struct.value.Point2dReader;
import org.neo4j.packstream.struct.value.Point3dReader;
import org.neo4j.packstream.struct.value.TimeReader;
import org.neo4j.values.storable.Value;

public class NativeStructRegistry extends AbstractMutableStructRegistry<Value> {
    private static final NativeStructRegistry INSTANCE = new NativeStructRegistry();

    private NativeStructRegistry() {
        this.registerReader(new DateReader());
        this.registerReader(new DateTimeReader());
        this.registerReader(new DateTimeZoneIdReader());
        this.registerReader(new DurationReader());
        this.registerReader(new LocalDateTimeReader());
        this.registerReader(new LocalTimeReader());
        this.registerReader(new Point2dReader());
        this.registerReader(new Point3dReader());
        this.registerReader(new TimeReader());
    }

    public static StructRegistry<Value> getInstance() {
        return INSTANCE;
    }
}
