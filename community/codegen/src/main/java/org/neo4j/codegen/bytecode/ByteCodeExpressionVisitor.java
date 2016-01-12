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
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Modifier;

import org.neo4j.codegen.Expression;
import org.neo4j.codegen.ExpressionVisitor;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodReference;
import org.neo4j.codegen.TypeReference;

import static org.neo4j.codegen.ByteCodeUtils.byteCodeName;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.typeName;

class ByteCodeExpressionVisitor implements ExpressionVisitor, Opcodes
{
    private final MethodVisitor methodVisitor;

    ByteCodeExpressionVisitor( MethodVisitor methodVisitor )
    {
        this.methodVisitor = methodVisitor;
    }

    @Override
    public void invoke( Expression target, MethodReference method, Expression[] arguments )
    {
        target.accept( this );
        for ( Expression argument : arguments )
        {
            argument.accept( this );
        }
        if ( Modifier.isInterface( method.owner().modifiers() ) )
        {
            methodVisitor
                    .visitMethodInsn( Opcodes.INVOKEINTERFACE, byteCodeName( method.owner() ), method.name(),
                            desc( method ),
                            true );
        }
        else if ( method.isConstructor() )
        {
            methodVisitor
                    .visitMethodInsn( Opcodes.INVOKESPECIAL, byteCodeName( method.owner() ), method.name(), desc( method ),
                            false );
        }
        else
        {
            methodVisitor
                    .visitMethodInsn( Opcodes.INVOKEVIRTUAL, byteCodeName( method.owner() ), method.name(), desc( method ),
                            false );
        }
    }

    @Override
    public void invoke( MethodReference method, Expression[] arguments )
    {
        for ( Expression argument : arguments )
        {
            argument.accept( this );
        }
        methodVisitor.visitMethodInsn( Opcodes.INVOKESTATIC, byteCodeName( method.owner() ), method.name(), desc( method ),
                false );
    }

    @Override
    public void load( LocalVariable variable )
    {
        switch ( variable.type().simpleName() )
        {
        case "int":
        case "byte":
        case "short":
        case "char":
        case "boolean":
            methodVisitor.visitVarInsn( Opcodes.ILOAD, variable.index() );
            break;
        case "long":
            methodVisitor.visitVarInsn( Opcodes.LLOAD, variable.index() );
            break;
        case "float":
            methodVisitor.visitVarInsn( Opcodes.FLOAD, variable.index() );
            break;
        case "double":
            methodVisitor.visitVarInsn( Opcodes.DLOAD, variable.index() );
            break;
        default:
            methodVisitor.visitVarInsn( Opcodes.ALOAD, variable.index() );
        }
    }

    @Override
    public void getField( Expression target, FieldReference field )
    {
        target.accept( this );
        methodVisitor
                .visitFieldInsn( Opcodes.GETFIELD, byteCodeName( field.owner() ), field.name(), typeName( field.type() ) );

    }

    @Override
    public void constant( Object value )
    {
        //TODO do type checking here
        methodVisitor.visitLdcInsn( value );
    }

    @Override
    public void getStatic( FieldReference field )
    {
        methodVisitor
                .visitFieldInsn( Opcodes.GETSTATIC, byteCodeName( field.owner() ), field.name(), typeName( field.type() ) );
    }

    @Override
    public void loadThis( String sourceName )
    {
        methodVisitor.visitVarInsn( Opcodes.ALOAD, 0 );
    }

    @Override
    public void newInstance( TypeReference type )
    {
        methodVisitor.visitTypeInsn( Opcodes.NEW, byteCodeName( type ) );
        // TODO: is this always true? typical use case is that you call the constructor directly
        // which pops the item of the stack so you need DUP in order to do stuff with it
        methodVisitor.visitInsn( Opcodes.DUP );
    }

    @Override
    public void not( Expression expression )
    {

    }

    @Override
    public void ternary( Expression test, Expression onTrue, Expression onFalse )
    {

    }

    @Override
    public void eq( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void or( Expression lhs, Expression rhs )
    {
        /*
         * something like:
         *
         * LOAD lhs
         * IF TRUE GOTO 0
         * LOAD rhs
         * IF FALSE GOTO 1
         *
         * 0:
         *  LOAD TRUE
         *  GOTO 2
         * 1:
         *  LOAD FALSE
         * 2:
         *  ...continue doing stuff
         */
        lhs.accept( this );
        Label l0 = new Label();
        methodVisitor.visitJumpInsn( IFNE, l0 );
        rhs.accept( this );
        Label l1 = new Label();
        methodVisitor.visitJumpInsn( IFEQ, l1 );
        methodVisitor.visitLabel( l0 );
        methodVisitor.visitInsn( ICONST_1 );
        Label l2 = new Label();
        methodVisitor.visitJumpInsn( GOTO, l2 );
        methodVisitor.visitInsn( ICONST_0 );
        methodVisitor.visitLabel( l2 );

    }

    @Override
    public void add( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void sub( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void cast( TypeReference type, Expression expression )
    {
        expression.accept( this );
        methodVisitor.visitTypeInsn( CHECKCAST, byteCodeName( type ) );
    }

    @Override
    public void newArray( TypeReference type, Expression... exprs )
    {
        pushInteger( exprs.length );
        createArray( type );
        for ( int i = 0; i < exprs.length; i++ )
        {
            methodVisitor.visitInsn( Opcodes.DUP );
            pushInteger( i );
            exprs[i].accept( this );
            arrayStore( type );
        }
    }

    private void pushInteger( int integer )
    {
        if ( integer < 6 && integer >= 0 )
        {
            methodVisitor.visitInsn( Opcodes.ICONST_0 + integer );
        }
        else if ( integer < Byte.MAX_VALUE && integer > Byte.MIN_VALUE )
        {
            methodVisitor.visitIntInsn( Opcodes.BIPUSH, integer );
        }
        else if ( integer < Short.MAX_VALUE && integer > Short.MIN_VALUE )
        {
            methodVisitor.visitIntInsn( Opcodes.SIPUSH, integer );
        }
        else
        {
            methodVisitor.visitLdcInsn( integer );
        }
    }

    private void createArray( TypeReference reference )
    {
        switch ( reference.name() )
        {
        case "int":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_INT );
            break;
        case "long":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_LONG );
            break;
        case "byte":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_BYTE );
            break;
        case "short":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_SHORT );
            break;
        case "char":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_CHAR );
            break;
        case "float":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_FLOAT );
            break;
        case "double":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_DOUBLE );
            break;
        case "boolean":
            methodVisitor.visitIntInsn( Opcodes.NEWARRAY, Opcodes.T_BOOLEAN );
            break;
        default:
            methodVisitor.visitTypeInsn( Opcodes.ANEWARRAY, byteCodeName( reference ) );

        }
    }

    private void arrayStore( TypeReference reference )
    {
        switch ( reference.name() )
        {
        case "int":
            methodVisitor.visitInsn( Opcodes.IASTORE );
            break;
        case "long":
            methodVisitor.visitInsn( Opcodes.LASTORE );
            break;
        case "byte":
            methodVisitor.visitInsn( Opcodes.BASTORE );
            break;
        case "short":
            methodVisitor.visitInsn( Opcodes.SASTORE );
            break;
        case "char":
            methodVisitor.visitInsn( Opcodes.CASTORE );
            break;
        case "float":
            methodVisitor.visitInsn( Opcodes.FASTORE );
            break;
        case "double":
            methodVisitor.visitInsn( Opcodes.DASTORE );
            break;
        case "boolean":
            methodVisitor.visitInsn( Opcodes.BASTORE );
            break;
        default:
            methodVisitor.visitInsn( Opcodes.AASTORE );

        }
    }
}
