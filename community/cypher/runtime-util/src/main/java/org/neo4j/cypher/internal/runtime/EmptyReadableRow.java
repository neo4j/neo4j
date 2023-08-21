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
package org.neo4j.cypher.internal.runtime;

import org.neo4j.cypher.internal.expressions.ASTCachedProperty;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import scala.Option;

public final class EmptyReadableRow implements ReadableRow {

    public static final EmptyReadableRow INSTANCE = new EmptyReadableRow();

    private EmptyReadableRow() {}

    @Override
    public Value getCachedProperty(ASTCachedProperty.RuntimeKey key) {
        return fail();
    }

    @Override
    public Value getCachedPropertyAt(int offset) {
        return fail();
    }

    @Override
    public void setCachedProperty(ASTCachedProperty.RuntimeKey key, Value value) {
        fail();
    }

    @Override
    public void setCachedPropertyAt(int offset, Value value) {
        fail();
    }

    @Override
    public long getLongAt(int offset) {
        return fail();
    }

    @Override
    public AnyValue getRefAt(int offset) {
        return fail();
    }

    @Override
    public AnyValue getByName(String name) {
        return fail();
    }

    @Override
    public Option<ResourceLinenumber> getLinenumber() {
        return fail();
    }

    private <T> T fail() {
        throw new IllegalStateException("Cannot access empty row");
    }
}
