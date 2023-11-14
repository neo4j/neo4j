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

import static org.objectweb.asm.Opcodes.ASTORE;

import org.neo4j.codegen.LocalVariable;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

public class Catch implements Block {
    private final MethodVisitor methodVisitor;
    private final Label failBody;
    private final Label after;

    public Catch(MethodVisitor methodVisitor, Label failBody, Label after) {
        this.methodVisitor = methodVisitor;
        this.failBody = failBody;
        this.after = after;
    }

    public void beginCatch(LocalVariable exception) {
        methodVisitor.visitLabel(failBody);
        methodVisitor.visitVarInsn(ASTORE, exception.index());
    }

    @Override
    public void endBlock() {
        methodVisitor.visitLabel(after);
    }
}
