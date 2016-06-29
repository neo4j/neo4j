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

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.Consumer;

import org.neo4j.codegen.Expression;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.TypeReference;

import static org.neo4j.codegen.ByteCodeUtils.byteCodeName;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.outerName;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.typeName;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.ATHROW;
import static org.objectweb.asm.Opcodes.DRETURN;
import static org.objectweb.asm.Opcodes.DSTORE;
import static org.objectweb.asm.Opcodes.FRETURN;
import static org.objectweb.asm.Opcodes.FSTORE;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.IFNONNULL;
import static org.objectweb.asm.Opcodes.IFNULL;
import static org.objectweb.asm.Opcodes.IRETURN;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.LRETURN;
import static org.objectweb.asm.Opcodes.LSTORE;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.RETURN;

class MethodByteCodeEmitter implements MethodEmitter
{
    private final MethodVisitor methodVisitor;
    private final MethodDeclaration declaration;
    private final ByteCodeExpressionVisitor expressionVisitor;
    private final TypeReference base;
    private Deque<Block> stateStack = new LinkedList<>();

    public MethodByteCodeEmitter( ClassWriter classWriter, MethodDeclaration declaration, TypeReference base )
    {
        this.declaration = declaration;
        this.base = base;
        for ( Parameter parameter : declaration.parameters() )
        {
            TypeReference type = parameter.type();
            if ( type.isInnerClass() )
            {
                classWriter.visitInnerClass( byteCodeName( type ), outerName( type ),
                        type.simpleName(), type.modifiers() );
            }
        }
        this.methodVisitor = classWriter.visitMethod( ACC_PUBLIC, declaration.name(), desc( declaration ),
                signature( declaration ), exceptions( declaration ) );
        this.methodVisitor.visitCode();
        this.expressionVisitor = new ByteCodeExpressionVisitor( this.methodVisitor );
        stateStack.push( new Method( methodVisitor, declaration.returnType().isVoid() ) );
    }

    @Override
    public void done()
    {
        methodVisitor.visitEnd();
    }

    @Override
    public void expression( Expression expression )
    {
        expression.accept( expressionVisitor );
    }

    @Override
    public void put( Expression target, FieldReference field, Expression value )
    {
        target.accept( expressionVisitor );
        value.accept( expressionVisitor );
        methodVisitor
                .visitFieldInsn( PUTFIELD, byteCodeName( field.owner() ), field.name(), typeName( field.type() ) );
    }

    @Override
    public void returns()
    {
        methodVisitor.visitInsn( RETURN );
    }

    @Override
    public void returns( Expression value )
    {
        value.accept( expressionVisitor );
        switch ( declaration.returnType().simpleName() )
        {
        case "int":
        case "byte":
        case "short":
        case "char":
        case "boolean":
            methodVisitor.visitInsn( IRETURN );
            break;
        case "long":
            methodVisitor.visitInsn( LRETURN );
            break;
        case "float":
            methodVisitor.visitInsn( FRETURN );
            break;
        case "double":
            methodVisitor.visitInsn( DRETURN );
            break;
        default:
            methodVisitor.visitInsn( ARETURN );
        }
    }

    @Override
    public void assign( LocalVariable variable, Expression value )
    {
        value.accept( expressionVisitor );
        switch ( variable.type().simpleName() )
        {
        case "int":
        case "byte":
        case "short":
        case "char":
        case "boolean":
            methodVisitor.visitVarInsn( ISTORE, variable.index() );
            break;
        case "long":
            methodVisitor.visitVarInsn( LSTORE, variable.index() );
            break;
        case "float":
            methodVisitor.visitVarInsn( FSTORE, variable.index() );
            break;
        case "double":
            methodVisitor.visitVarInsn( DSTORE, variable.index() );
            break;
        default:
            methodVisitor.visitVarInsn( ASTORE, variable.index() );
        }
    }

    @Override
    public void beginWhile( Expression...tests )
    {
        Label l0 = new Label();
        methodVisitor.visitLabel( l0 );
        Label l1 = new Label();
        for ( Expression test : tests )
        {
            test.accept( expressionVisitor );
            methodVisitor.visitJumpInsn( IFEQ, l1 );
        }

        stateStack.push( new While( methodVisitor, l0, l1  ) );
    }

    @Override
    public void beginIf( Expression... tests )
    {
        beginConditional( IFEQ, tests );
    }

    @Override
    public void beginIfNot( Expression... tests )
    {
        beginConditional( IFNE, tests );
    }

    @Override
    public void beginIfNull( Expression...tests )
    {
        beginConditional( IFNONNULL, tests );
    }

    @Override
    public void beginIfNonNull( Expression...tests )
    {
        beginConditional( IFNULL, tests );
    }

    @Override
    public void endBlock()
    {
        if ( stateStack.isEmpty() )
        {
            throw new IllegalStateException( "Unbalanced blocks" );
        }
        stateStack.pop().endBlock();
    }

    @Override
    public <T> void tryCatchBlock( Consumer<T> body, Consumer<T> handler, LocalVariable exception, T block )
    {
        Label l0 = new Label();
        Label l1 = new Label();
        Label l2 = new Label();
        methodVisitor.visitTryCatchBlock( l0, l1, l2,
                byteCodeName( exception.type() ) );
        methodVisitor.visitLabel( l0 );
        body.accept( block );
        methodVisitor.visitLabel( l1 );
        Label l3 = new Label();
        methodVisitor.visitJumpInsn( GOTO, l3 );
        //handle catch
        methodVisitor.visitLabel( l2 );
        methodVisitor.visitVarInsn( ASTORE, exception.index() );

        handler.accept( block );
        methodVisitor.visitLabel( l3 );
    }

    @Override
    public void throwException( Expression exception )
    {
        exception.accept( expressionVisitor );
        methodVisitor.visitInsn( ATHROW );
    }

    @Override
    public void declare( LocalVariable local )
    {
        //declare is a noop bytecode wise
    }

    @Override
    public void assignVariableInScope( LocalVariable local, Expression value )
    {
        //these are equivalent when it comes to bytecode
        assign( local, value );
    }

    private void beginConditional(int op, Expression[] tests)
    {
        Label l0 = new Label();
        for ( Expression test : tests )
        {
            test.accept( expressionVisitor );
            methodVisitor.visitJumpInsn( op, l0 );
        }
        stateStack.push(new If(methodVisitor, l0));
    }
}
