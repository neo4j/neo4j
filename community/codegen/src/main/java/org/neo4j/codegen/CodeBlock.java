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

import static org.neo4j.codegen.LocalVariables.copy;
import static org.neo4j.codegen.MethodReference.methodReference;
import static org.neo4j.codegen.TypeReference.typeReference;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Representation of a code block. Wraps a MethodWriter and adds management of local variables, and
 * the ability to create nested code blocks.
 */
public class CodeBlock implements AutoCloseable {
    final ClassGenerator clazz;
    protected MethodWriter writer;
    private final CodeBlock parent;
    private boolean done;
    private final boolean continuableBlock;

    protected LocalVariables localVariables = new LocalVariables();

    protected CodeBlock(CodeBlock parent) {
        this(parent, parent.continuableBlock);
    }

    private CodeBlock(CodeBlock parent, boolean continuableBlock) {
        this.clazz = parent.clazz;
        this.writer = parent.writer;
        parent.writer = InvalidState.IN_SUB_BLOCK;
        this.parent = parent;
        // copy over local variables from parent
        this.localVariables = copy(parent.localVariables);
        this.continuableBlock = continuableBlock;
    }

    CodeBlock(ClassGenerator clazz, MethodWriter writer, Parameter... parameters) {
        this.clazz = clazz;
        this.writer = writer;
        this.parent = null;
        this.continuableBlock = false;
        if (!writer.isStatic()) {
            localVariables.createNew(clazz.handle(), "this");
        }
        for (Parameter parameter : parameters) {
            localVariables.createNew(parameter.type(), parameter.name());
        }
    }

    public ClassGenerator classGenerator() {
        return clazz;
    }

    public CodeBlock parent() {
        return parent;
    }

    @Override
    public void close() {
        endBlock();
        if (parent != null) {
            parent.writer = writer;
        } else {
            writer.done();
        }
        this.writer = InvalidState.BLOCK_CLOSED;
    }

    protected void endBlock() {
        if (!done) {
            writer.endBlock();
            done = true;
        }
    }

    public void expression(Expression expression) {
        writer.expression(expression);
    }

    public LocalVariable local(String name) {
        return localVariables.get(name);
    }

    public LocalVariable declare(TypeReference type, String name) {
        LocalVariable local = localVariables.createNew(type, name);
        writer.declare(local);
        return local;
    }

    public void assign(LocalVariable local, Expression value) {
        writer.assignVariableInScope(local, value);
    }

    public void assign(Class<?> type, String name, Expression value) {
        assign(typeReference(type), name, value);
    }

    public void assign(TypeReference type, String name, Expression value) {
        LocalVariable variable = localVariables.createNew(type, name);
        writer.assign(variable, value);
    }

    public void put(Expression target, FieldReference field, Expression value) {
        writer.put(target, field, value);
    }

    public void putStatic(FieldReference field, Expression value) {
        writer.putStatic(field, value);
    }

    public Expression self() {
        return load("this");
    }

    public Expression load(String name) {
        return Expression.load(local(name));
    }

    /*
     * Foreach is just syntactic sugar for a while loop.
     */
    public CodeBlock forEach(Parameter local, Expression iterable) {
        String iteratorName = local.name() + "Iter";

        assign(
                Iterator.class,
                iteratorName,
                Expression.invoke(
                        iterable, MethodReference.methodReference(Iterable.class, Iterator.class, "iterator")));
        CodeBlock block = whileLoop(
                Expression.invoke(load(iteratorName), methodReference(Iterator.class, boolean.class, "hasNext")));
        block.assign(
                local.type(),
                local.name(),
                Expression.cast(
                        local.type(),
                        Expression.invoke(
                                block.load(iteratorName), methodReference(Iterator.class, Object.class, "next"))));

        return block;
    }

    public CodeBlock whileLoop(Expression test) {
        writer.beginWhile(test, null);
        return new CodeBlock(this, true);
    }

    public CodeBlock whileLoop(Expression test, String labelName) {
        writer.beginWhile(test, labelName);
        return new CodeBlock(this, true);
    }

    public CodeBlock ifStatement(Expression test) {
        writer.beginIf(test);
        return new CodeBlock(this);
    }

    public void ifElseStatement(Expression test, Consumer<CodeBlock> onTrue, Consumer<CodeBlock> onFalse) {
        writer.ifElseStatement(test, onTrue, onFalse, this);
    }

    public CodeBlock block() {
        writer.beginBlock();
        return new CodeBlock(this);
    }

    public TryCatchCodeBlock tryCatch(Consumer<CodeBlock> onError, Parameter exception) {
        writer.beginTry(exception);
        return new TryCatchCodeBlock(this, onError, exception);
    }

    public void returns() {
        writer.returns();
    }

    public void returns(Expression value) {
        writer.returns(value);
    }

    public void continueIfPossible() {
        if (continuableBlock) {
            writer.continues();
        }
    }

    public void breaks(String labelName) {
        writer.breaks(labelName);
    }

    public void throwException(Expression exception) {
        writer.throwException(exception);
    }

    public TypeReference owner() {
        return clazz.handle();
    }
}
