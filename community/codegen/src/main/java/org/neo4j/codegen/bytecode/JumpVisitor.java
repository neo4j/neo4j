/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.codegen.Expression;
import org.neo4j.codegen.ExpressionVisitor;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodReference;
import org.neo4j.codegen.TypeReference;

import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.IFNONNULL;
import static org.objectweb.asm.Opcodes.IFNULL;

class JumpVisitor implements ExpressionVisitor
{
    private final ExpressionVisitor eval;
    private final MethodVisitor methodVisitor;
    private final Label target;

    JumpVisitor( ExpressionVisitor eval, MethodVisitor methodVisitor, Label target )
    {
        this.eval = eval;
        this.methodVisitor = methodVisitor;
        this.target = target;
    }

    @Override
    public void invoke( Expression target, MethodReference method, Expression[] arguments )
    {
        eval.invoke( target, method, arguments );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void invoke( MethodReference method, Expression[] arguments )
    {
        eval.invoke( method, arguments );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void load( LocalVariable variable )
    {
        eval.load( variable );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void getField( Expression target, FieldReference field )
    {
        eval.getField( target, field );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void constant( Object value )
    {
        eval.constant( value );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void getStatic( FieldReference field )
    {
        eval.getStatic( field );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void not( Expression expression )
    {
        expression.accept( eval );
        methodVisitor.visitJumpInsn( IFNE, this.target );
    }

    @Override
    public void ternary( Expression test, Expression onTrue, Expression onFalse )
    {
        eval.ternary( test, onTrue, onFalse );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void equal( Expression lhs, Expression rhs )
    {
        eval.equal( lhs, rhs );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void notEqual( Expression lhs, Expression rhs )
    {
        eval.equal( lhs, rhs );
        methodVisitor.visitJumpInsn( IFNE, this.target );
    }

    @Override
    public void isNull( Expression expression )
    {
        expression.accept( eval );
        methodVisitor.visitJumpInsn( IFNONNULL, this.target );
    }

    @Override
    public void notNull( Expression expression )
    {
        expression.accept( eval );
        methodVisitor.visitJumpInsn( IFNULL, this.target );
    }

    @Override
    public void or( Expression... expressions )
    {
        eval.or( expressions );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void and( Expression... expressions )
    {
        for ( Expression expression : expressions )
        {
            expression.accept( this );
        }
    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {
        eval.gt( lhs, rhs );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void gte( Expression lhs, Expression rhs )
    {
        eval.gte( lhs, rhs );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void lt( Expression lhs, Expression rhs )
    {
        eval.lt( lhs, rhs );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void lte( Expression lhs, Expression rhs )
    {
        eval.lte( lhs, rhs );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void unbox( Expression expression )
    {
        eval.unbox( expression );
        methodVisitor.visitJumpInsn( IFEQ, this.target );
    }

    @Override
    public void loadThis( String sourceName )
    {
        throw new IllegalArgumentException( "'" + sourceName + "' is not a boolean expression" );
    }

    @Override
    public void newInstance( TypeReference type )
    {
        throw new IllegalArgumentException( "'new' is not a boolean expression" );
    }

    @Override
    public void add( Expression lhs, Expression rhs )
    {
        throw new IllegalArgumentException( "'+' is not a boolean expression" );
    }

    @Override
    public void subtract( Expression lhs, Expression rhs )
    {
        throw new IllegalArgumentException( "'-' is not a boolean expression" );
    }

    @Override
    public void multiply( Expression lhs, Expression rhs )
    {
        throw new IllegalArgumentException( "'*' is not a boolean expression" );
    }

    @Override
    public void cast( TypeReference type, Expression expression )
    {
        throw new IllegalArgumentException( "cast is not a boolean expression" );
    }

    @Override
    public void newArray( TypeReference type, Expression... constants )
    {
        throw new IllegalArgumentException( "'new' (array) is not a boolean expression" );
    }

    @Override
    public void longToDouble( Expression expression )
    {
        throw new IllegalArgumentException( "cast is not a boolean expression" );
    }

    @Override
    public void pop( Expression expression )
    {
        throw new IllegalArgumentException( "pop is not a boolean expression" );
    }

    @Override
    public void box( Expression expression )
    {
        throw new IllegalArgumentException( "box is not a boolean expression" );
    }
}
