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
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.codegen.BaseExpressionVisitor;
import org.neo4j.codegen.CatchClause;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.LocalVariables;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.MethodReference;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.Resource;
import org.neo4j.codegen.TypeReference;

import static org.neo4j.codegen.ByteCodeUtils.byteCodeName;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.outerName;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.typeName;
import static org.neo4j.codegen.MethodReference.methodReference;

class MethodByteCodeEmitter implements MethodEmitter, Opcodes
{
    private final MethodVisitor methodVisitor;
    private final MethodDeclaration declaration;
    private final ByteCodeExpressionVisitor expressionVisitor;
    private boolean calledSuper = false;
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
        callSuperIfNecessary();

        methodVisitor.visitEnd();
    }

    @Override
    public void expression( Expression expression )
    {
        callSuperIfNecessary( expression );
        expression.accept( expressionVisitor );
    }

    @Override
    public void put( Expression target, FieldReference field, Expression value )
    {
        callSuperIfNecessary();

        target.accept( expressionVisitor );
        value.accept( expressionVisitor );
        methodVisitor
                .visitFieldInsn( PUTFIELD, byteCodeName( field.owner() ), field.name(), typeName( field.type() ) );
    }

    @Override
    public void returns()
    {
        callSuperIfNecessary();

        methodVisitor.visitInsn( RETURN );
    }

    @Override
    public void returns( Expression value )
    {
        callSuperIfNecessary();

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
        callSuperIfNecessary();
        value.accept( expressionVisitor );
        switch ( variable.type().simpleName() )
        {
        case "int":
        case "byte":
        case "short":
        case "char":
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
    public void beginWhile( Expression test )
    {
        callSuperIfNecessary();
        Label l0 = new Label();
        methodVisitor.visitLabel( l0 );
        test.accept( expressionVisitor );
        Label l1 = new Label();
        methodVisitor.visitJumpInsn( IFEQ, l1 );

        stateStack.push( new While( methodVisitor, l0, l1  ) );
    }

    @Override
    public void beginIf( Expression test )
    {
        callSuperIfNecessary();
        test.accept( expressionVisitor );

        Label l0 = new Label();
        methodVisitor.visitJumpInsn( IFEQ, l0 );

        stateStack.push(new If(methodVisitor, l0));
    }


    @Override
    public void endBlock()
    {
        callSuperIfNecessary();

        if ( stateStack.isEmpty() )
        {
            throw new IllegalStateException( "Unbalanced blocks" );
        }
        stateStack.pop().endBlock();
    }

    @Override
    public void tryCatchBlock( List<Consumer<MethodEmitter>> body, List<CatchClause> catchClauses,
            List<Consumer<MethodEmitter>> finalClauses, LocalVariables localVariables, Resource... resources )
    {
        callSuperIfNecessary();
        //Note we are not giving the same guarantee as built in try-with-resources where it also suppress
        //exceptions in the finally block. For now this is sufficient but we could revisit in the future.
        //Here we are essentially rewriting to:
        //
        //  Resource resource = ...
        //  try
        //  {
        //      ...
        //  }
        //  finally
        //  {
        //      ...
        //      resource.close();
        //  }
        //
        List<Consumer<MethodEmitter>> updatedFinalClauses = resources.length == 0 ?
                                                            finalClauses : new ArrayList<>( finalClauses );
        for ( Resource resource : resources )
        {
            LocalVariable variable = localVariables.createNew( resource.type(), resource.name() );
            assign( variable, resource.producer() );

            MethodReference close = methodReference( resource.type(), TypeReference.VOID, "close" );
            updatedFinalClauses.add( ( e ) -> e.expression(
                    Expression.invoke( Expression.load( variable ), close ) ) );
        }

        if ( !catchClauses.isEmpty() )
        {
           tryCatchFinally( body, catchClauses, updatedFinalClauses, localVariables );
        }
        else
        {
            tryFinally( body, updatedFinalClauses, localVariables );
        }
    }

    /*
     * Handle try-block with no catch clause
     */
    private void tryFinally(List<Consumer<MethodEmitter>> body,
            List<Consumer<MethodEmitter>> finalClauses, LocalVariables localVariables)
    {
        Label startLabel = new Label();
        Label endLabel = new Label();
        Label continueLabel = new Label();
        //if we have a simple try {} finally {} we only need to run finally after body
        //and make sure we catch any errors and then run finally and rethrow
        Label handlerLabel = new Label();
        methodVisitor.visitTryCatchBlock( startLabel, endLabel, handlerLabel, null );
        methodVisitor.visitLabel( startLabel );
        body.forEach( e -> e.accept( this ) );
        methodVisitor.visitLabel( endLabel );
        //run finally on success
        finalClauses.forEach( e -> e.accept( this ) );

        int indexToStoreException = localVariables.nextIndex();
        //catch error and run finally
        methodVisitor.visitJumpInsn( GOTO, continueLabel );
        methodVisitor.visitLabel( handlerLabel );
        //store error
        methodVisitor.visitVarInsn( ASTORE, indexToStoreException );
        finalClauses.forEach( e -> e.accept( this ) );
        //catch and rethrow
        methodVisitor.visitVarInsn( ALOAD, indexToStoreException );
        methodVisitor.visitInsn( ATHROW );
        methodVisitor.visitLabel( continueLabel );
    }

    /*
     * Handle a full try-catch-finally block.
     */
    private void tryCatchFinally(List<Consumer<MethodEmitter>> body, List<CatchClause> catchClauses,
            List<Consumer<MethodEmitter>> finalClauses, LocalVariables localVariables)
    {
        Label startLabel = new Label();
        Label endLabel = new Label();
        Label continueLabel = new Label();
        //if we have catch clauses we need to catch error, run body create labels for each catch clauses
        //we also need to make sure that the final clauses run both after body and after each catch block
        //and also make sure if any of the catch clauses fail we still run finally

        //generate try-catch block for each catch clause
        List<Label> handlerLabels = new ArrayList<>( catchClauses.size() );
        catchClauses.forEach(
                c -> {
                    Label handlerLabel = new Label();
                    methodVisitor.visitTryCatchBlock( startLabel, endLabel, handlerLabel,
                            byteCodeName( c.exception().type() ) );
                    handlerLabels.add( handlerLabel );
                } );

        //generate try-catch block for handling finally
        List<Label> endLabelsForFinal = new ArrayList<>( catchClauses.size() );
        Label handlerLabelFinal = new Label();
        Label endFinal = new Label();
        if ( !finalClauses.isEmpty() )
        {
            methodVisitor.visitTryCatchBlock( startLabel, endLabel, handlerLabelFinal, null );
            for ( Label handlerLabel : handlerLabels )
            {
                Label endCatchLabel = new Label();
                methodVisitor.visitTryCatchBlock( handlerLabel, endCatchLabel, handlerLabelFinal, null );
                endLabelsForFinal.add( endCatchLabel );
            }
            methodVisitor.visitTryCatchBlock( handlerLabelFinal, endFinal, handlerLabelFinal, null );
        }

        //run body
        methodVisitor.visitLabel( startLabel );
        body.forEach( e -> e.accept( this ) );
        methodVisitor.visitLabel( endLabel );

        //if body succeeded we need to run final
        finalClauses.forEach( e -> e.accept( this ) );

        //handle catch
        int indexToStoreException = localVariables.nextIndex();
        for ( int i = 0; i < catchClauses.size(); i++ )
        {
            CatchClause catchClause = catchClauses.get( i );
            Label handlerLabel = handlerLabels.get( i );
            methodVisitor.visitJumpInsn( GOTO, continueLabel );
            methodVisitor.visitLabel( handlerLabel );
            //store exception for later use
            methodVisitor.visitVarInsn( ASTORE, indexToStoreException );
            catchClause.actions().forEach( e -> e.accept( this ) );

            //run final clauses on failure
            if ( !finalClauses.isEmpty() )
            {
                methodVisitor.visitLabel( endLabelsForFinal.get( i ) );
                finalClauses.forEach( e -> e.accept( this ) );
            }
        }

        if ( !finalClauses.isEmpty() )
        {
            indexToStoreException = localVariables.nextIndex();
            //need to catch exceptions on finally
            methodVisitor.visitJumpInsn( GOTO, continueLabel );
            methodVisitor.visitLabel( handlerLabelFinal );
            methodVisitor.visitVarInsn( ASTORE, indexToStoreException );
            methodVisitor.visitLabel( endFinal );

            //run finally again
            finalClauses.forEach( e -> e.accept( this ) );

            //rethrow
            methodVisitor.visitVarInsn( ALOAD, indexToStoreException );
            methodVisitor.visitInsn( ATHROW );
        }

        //continue
        methodVisitor.visitLabel( continueLabel );
    }

    @Override
    public void throwException( Expression exception )
    {
        callSuperIfNecessary();
        exception.accept( expressionVisitor );
        methodVisitor.visitInsn( ATHROW );
    }

    @Override
    public void declare( LocalVariable local )
    {
        callSuperIfNecessary();

        //declare is a noop bytecode wise
    }

    @Override
    public void assignVariableInScope( LocalVariable local, Expression value )
    {
        //these are equivalent when it comes to bytecode
        assign( local, value );
    }

    /*
     * Make sure we call default super constructor
     */
    private void callSuperIfNecessary()
    {
        if ( needsToCallSuperConstructor() )
        {
            callSuper();
        }
    }

    /*
     * Checks if expression contains call to super constructor
     * otherwise generates call to default super constructor.
     *
     */
    private void callSuperIfNecessary( Expression expression )
    {
        if ( !needsToCallSuperConstructor() )
        {
            return;
        }

        expression.accept( new BaseExpressionVisitor()
        {
            @Override
            public void invoke( Expression target, MethodReference method, Expression[] arguments )
            {
                calledSuper = method.isConstructor() && loadsSuper( target );
            }
        } );

        callSuperIfNecessary();
    }

    private void callSuper()
    {
        methodVisitor.visitVarInsn( ALOAD, 0 );
        methodVisitor.visitMethodInsn( INVOKESPECIAL, byteCodeName( base ), "<init>", "()V", false );
        calledSuper = true;
    }

    private boolean needsToCallSuperConstructor()
    {
        return declaration.isConstructor() && !calledSuper;
    }


    private boolean loadsSuper( Expression expression )
    {
        final boolean[] loadsSuper = new boolean[]{false};
        expression.accept( new BaseExpressionVisitor()
        {
            @Override
            public void loadThis( String sourceName )
            {
                loadsSuper[0] = "super".equals( sourceName );
            }
        } );

        return loadsSuper[0];
    }

}
