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
package org.neo4j.codegen.bytecode;

import static org.neo4j.codegen.ByteCodeUtils.byteCodeName;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.outerName;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.typeName;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.ATHROW;
import static org.objectweb.asm.Opcodes.DRETURN;
import static org.objectweb.asm.Opcodes.DSTORE;
import static org.objectweb.asm.Opcodes.FRETURN;
import static org.objectweb.asm.Opcodes.FSTORE;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.IRETURN;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.LRETURN;
import static org.objectweb.asm.Opcodes.LSTORE;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.PUTSTATIC;
import static org.objectweb.asm.Opcodes.RETURN;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.Consumer;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.ExpressionVisitor;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.codegen.MethodWriter;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.TypeReference;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

class ByteCodeMethodWriter implements MethodWriter {
    private final MethodVisitor methodVisitor;
    private final MethodDeclaration declaration;
    private final ExpressionVisitor expressionVisitor;
    private final Deque<Block> stateStack = new LinkedList<>();

    ByteCodeMethodWriter(ClassVisitor classVisitor, MethodDeclaration declaration, TypeReference ignore) {
        this.declaration = declaration;
        for (Parameter parameter : declaration.parameters()) {
            TypeReference type = parameter.type();
            if (type.isInnerClass() && !type.isArray()) {
                classVisitor.visitInnerClass(byteCodeName(type), outerName(type), type.simpleName(), type.modifiers());
            }
        }
        int access = declaration.isStatic() ? ACC_PUBLIC + ACC_STATIC : ACC_PUBLIC;
        this.methodVisitor = classVisitor.visitMethod(
                access, declaration.name(), desc(declaration), signature(declaration), exceptions(declaration));
        this.methodVisitor.visitCode();
        this.expressionVisitor = new ByteCodeExpressionVisitor(this.methodVisitor);
        stateStack.push(new Method(methodVisitor, declaration.returnType().isVoid()));
    }

    @Override
    public boolean isStatic() {
        return declaration.isStatic();
    }

    @Override
    public void done() {
        methodVisitor.visitEnd();
    }

    @Override
    public void expression(Expression expression) {
        expression.accept(expressionVisitor);
    }

    @Override
    public void put(Expression target, FieldReference field, Expression value) {
        target.accept(expressionVisitor);
        value.accept(expressionVisitor);
        methodVisitor.visitFieldInsn(PUTFIELD, byteCodeName(field.owner()), field.name(), typeName(field.type()));
    }

    @Override
    public void putStatic(FieldReference field, Expression value) {
        value.accept(expressionVisitor);
        methodVisitor.visitFieldInsn(PUTSTATIC, byteCodeName(field.owner()), field.name(), typeName(field.type()));
    }

    @Override
    public void returns() {
        methodVisitor.visitInsn(RETURN);
    }

    @Override
    public void returns(Expression value) {
        value.accept(expressionVisitor);
        if (declaration.returnType().isPrimitive()) {
            switch (declaration.returnType().name()) {
                case "int":
                case "byte":
                case "short":
                case "char":
                case "boolean":
                    methodVisitor.visitInsn(IRETURN);
                    break;
                case "long":
                    methodVisitor.visitInsn(LRETURN);
                    break;
                case "float":
                    methodVisitor.visitInsn(FRETURN);
                    break;
                case "double":
                    methodVisitor.visitInsn(DRETURN);
                    break;
                default:
                    methodVisitor.visitInsn(ARETURN);
            }
        } else {
            methodVisitor.visitInsn(ARETURN);
        }
    }

    @Override
    public void continues() {
        for (Block block : stateStack) {
            if (block instanceof While) {
                ((While) block).continueBlock();
                return;
            }
        }
        throw new IllegalStateException("Found no block to continue");
    }

    @Override
    public void breaks(String labelName) {
        for (Block block : stateStack) {
            if (block instanceof While) {
                if (((While) block).breakBlock(labelName)) {
                    return;
                }
            }
        }
        throw new IllegalStateException("Found no block to break out of with label " + labelName);
    }

    @Override
    public void assign(LocalVariable variable, Expression value) {
        value.accept(expressionVisitor);
        if (variable.type().isPrimitive()) {
            switch (variable.type().name()) {
                case "int":
                case "byte":
                case "short":
                case "char":
                case "boolean":
                    methodVisitor.visitVarInsn(ISTORE, variable.index());
                    break;
                case "long":
                    methodVisitor.visitVarInsn(LSTORE, variable.index());
                    break;
                case "float":
                    methodVisitor.visitVarInsn(FSTORE, variable.index());
                    break;
                case "double":
                    methodVisitor.visitVarInsn(DSTORE, variable.index());
                    break;
                default:
                    methodVisitor.visitVarInsn(ASTORE, variable.index());
            }
        } else {
            methodVisitor.visitVarInsn(ASTORE, variable.index());
        }
    }

    @Override
    public void beginWhile(Expression test, String labelName) {
        Label repeat = new Label();
        Label done = new Label();
        methodVisitor.visitLabel(repeat);
        test.accept(new JumpVisitor(expressionVisitor, methodVisitor, done));

        stateStack.push(new While(methodVisitor, repeat, done, labelName));
    }

    @Override
    public void beginIf(Expression test) {
        Label after = new Label();
        test.accept(new JumpVisitor(expressionVisitor, methodVisitor, after));
        stateStack.push(new If(methodVisitor, after));
    }

    @Override
    public <T> void ifElseStatement(Expression test, Consumer<T> onTrue, Consumer<T> onFalse, T block) {
        Label onFailLabel = new Label();
        Label doneLabel = new Label();
        test.accept(new JumpVisitor(expressionVisitor, methodVisitor, onFailLabel));
        // test true, evaluate and GOTO done
        onTrue.accept(block);
        methodVisitor.visitJumpInsn(GOTO, doneLabel);

        // test false, go to here
        methodVisitor.visitLabel(onFailLabel);
        onFalse.accept(block);

        // goto here when onTrue body is done
        methodVisitor.visitLabel(doneLabel);
    }

    @Override
    public void beginBlock() {
        stateStack.push(() -> {});
    }

    @Override
    public void endBlock() {
        if (stateStack.isEmpty()) {
            throw new IllegalStateException("Unbalanced blocks");
        }
        stateStack.pop().endBlock();
    }

    @Override
    public void beginTry(Parameter exception) {
        Label start = new Label();
        Label tryBody = new Label();
        Label failBody = new Label();
        Label after = new Label();
        methodVisitor.visitLabel(start);
        stateStack.push(new Catch(methodVisitor, failBody, after));
        stateStack.push(new Try(methodVisitor, start, tryBody, failBody, after, exception));
    }

    @Override
    public void beginCatch(LocalVariable exception) {
        if (stateStack.peek() instanceof Catch c) {
            c.beginCatch(exception);
        } else {
            throw new IllegalStateException("Mismatched try-catch statement");
        }
    }

    @Override
    public void throwException(Expression exception) {
        exception.accept(expressionVisitor);
        methodVisitor.visitInsn(ATHROW);
    }

    @Override
    public void declare(LocalVariable local) {
        // declare is a noop bytecode wise
    }

    @Override
    public void assignVariableInScope(LocalVariable local, Expression value) {
        // these are equivalent when it comes to bytecode
        assign(local, value);
    }
}
