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
package org.neo4j.codegen;

import java.util.function.Consumer;

/**
 * Writes methods into some serialized representation.
 */
public interface MethodWriter {
    boolean isStatic();

    void done();

    void expression(Expression expression);

    void put(Expression target, FieldReference field, Expression value);

    void putStatic(FieldReference field, Expression value);

    void returns();

    void returns(Expression value);

    void continues();

    void breaks(String labelName);

    void assign(LocalVariable local, Expression value);

    void beginWhile(Expression test, String labelName);

    void beginIf(Expression test);

    void beginBlock();

    void beginTry(Parameter exception);

    void beginCatch(LocalVariable exception);

    void endBlock();

    <T> void ifElseStatement(Expression test, Consumer<T> onTrue, Consumer<T> onFalse, T block);

    void throwException(Expression exception);

    void declare(LocalVariable local);

    void assignVariableInScope(LocalVariable local, Expression value);
}
