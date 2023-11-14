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

class InvalidState implements MethodWriter {
    public static final ClassWriter CLASS_DONE = new ClassWriter() {
        @Override
        public MethodWriter method(MethodDeclaration method) {
            throw new IllegalStateException("class done");
        }

        @Override
        public void field(FieldReference field, Expression value) {
            throw new IllegalStateException("class done");
        }

        @Override
        public void done() {
            throw new IllegalStateException("class done");
        }
    };
    public static final MethodWriter BLOCK_CLOSED = new InvalidState("this block has been closed");
    public static final MethodWriter IN_SUB_BLOCK = new InvalidState("currently generating a sub-block of this block");
    private final String reason;

    private InvalidState(String reason) {
        this.reason = reason;
    }

    @Override
    public boolean isStatic() {
        throw new IllegalStateException(reason);
    }

    @Override
    public void done() {
        throw new IllegalStateException(reason);
    }

    @Override
    public void expression(Expression expression) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void put(Expression target, FieldReference field, Expression value) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void putStatic(FieldReference field, Expression value) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void returns() {
        throw new IllegalStateException(reason);
    }

    @Override
    public void returns(Expression value) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void continues() {
        throw new IllegalStateException(reason);
    }

    @Override
    public void breaks(String labelName) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void assign(LocalVariable variable, Expression value) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void beginWhile(Expression test, String labelName) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void beginIf(Expression test) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void beginBlock() {
        throw new IllegalStateException(reason);
    }

    @Override
    public void endBlock() {
        throw new IllegalStateException(reason);
    }

    @Override
    public <T> void ifElseStatement(Expression test, Consumer<T> onTrue, Consumer<T> onFalse, T block) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void throwException(Expression exception) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void declare(LocalVariable local) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void assignVariableInScope(LocalVariable local, Expression value) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void beginTry(Parameter exception) {
        throw new IllegalStateException(reason);
    }

    @Override
    public void beginCatch(LocalVariable exception) {
        throw new IllegalStateException(reason);
    }
}
