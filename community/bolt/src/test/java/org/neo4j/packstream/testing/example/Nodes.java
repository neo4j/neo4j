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
package org.neo4j.packstream.testing.example;

import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualValues;

public final class Nodes {

    private Nodes() {}

    public static final NodeValue ALICE = nodeValue(
            1001L,
            "n1",
            stringArray("Person", "Employee"),
            VirtualValues.map(
                    new String[] {"name", "age"}, new AnyValue[] {stringValue("Alice"), Values.longValue(33L)}));
    public static final NodeValue BOB = nodeValue(
            1002L,
            "n2",
            stringArray("Person", "Employee"),
            VirtualValues.map(
                    new String[] {"name", "age"}, new AnyValue[] {stringValue("Bob"), Values.longValue(44L)}));
    public static final NodeValue CAROL =
            nodeValue(1003L, "n3", stringArray("Person"), VirtualValues.map(new String[] {"name"}, new AnyValue[] {
                stringValue("Carol")
            }));
    public static final NodeValue DAVE = nodeValue(
            1004L, "n4", stringArray(), VirtualValues.map(new String[] {"name"}, new AnyValue[] {stringValue("Dave")}));
}
