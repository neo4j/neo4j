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
import static org.objectweb.asm.Opcodes.GOTO;

import org.neo4j.codegen.Parameter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

public class Try implements Block {
    private final MethodVisitor methodVisitor;
    private final Label start;
    private final Label tryBody;
    private final Label failBody;
    private final Label after;

    private final Parameter exception;

    public Try(
            MethodVisitor methodVisitor, Label start, Label tryBody, Label failBody, Label after, Parameter exception) {
        this.methodVisitor = methodVisitor;
        this.start = start;
        this.tryBody = tryBody;
        this.failBody = failBody;
        this.after = after;
        this.exception = exception;
    }

    public Parameter exception() {
        return exception;
    }

    public Label after() {
        return after;
    }

    @Override
    public void endBlock() {
        methodVisitor.visitTryCatchBlock(start, tryBody, failBody, byteCodeName(exception.type()));
        methodVisitor.visitLabel(tryBody);
        methodVisitor.visitJumpInsn(GOTO, after);
    }
}
