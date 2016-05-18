/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.codegen.bytecode;


import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import static org.objectweb.asm.Opcodes.GOTO;

public class While implements Block
{
    private final MethodVisitor methodVisitor;
    private final Label l0;
    private final Label l1;

    public While( MethodVisitor methodVisitor, Label l0, Label l1 )
    {
        this.methodVisitor = methodVisitor;
        this.l0 = l0;
        this.l1 = l1;
    }

    @Override
    public void endBlock()
    {
        methodVisitor.visitJumpInsn( GOTO, l0 );
        methodVisitor.visitLabel( l1 );
    }
}
