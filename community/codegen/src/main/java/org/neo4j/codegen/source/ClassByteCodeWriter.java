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
package org.neo4j.codegen.source;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.codegen.BaseExpressionVisitor;
import org.neo4j.codegen.ByteCodes;
import org.neo4j.codegen.ClassEmitter;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.ExpressionVisitor;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.MethodReference;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.Resource;
import org.neo4j.codegen.TypeReference;

import static org.neo4j.codegen.ByteCodeUtils.byteCodeName;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.typeName;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.DLOAD;
import static org.objectweb.asm.Opcodes.DRETURN;
import static org.objectweb.asm.Opcodes.FLOAD;
import static org.objectweb.asm.Opcodes.FRETURN;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.IRETURN;
import static org.objectweb.asm.Opcodes.LLOAD;
import static org.objectweb.asm.Opcodes.LRETURN;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.PUTSTATIC;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_8;

class ClassByteCodeWriter implements ClassEmitter
{
    private final ClassWriter classWriter;
    private final TypeReference type;
    private final Map<FieldReference,Expression> staticFields = new HashMap<>();
    private final TypeReference base;

    ClassByteCodeWriter( TypeReference type, TypeReference base, TypeReference[] interfaces )
    {
        this.classWriter = new ClassWriter( ClassWriter.COMPUTE_MAXS );
        String[] iNames = new String[interfaces.length];
        for ( int i = 0; i < interfaces.length; i++ )
        {
            iNames[i] = byteCodeName( interfaces[i].name() );
        }
        classWriter.visit( V1_8, ACC_PUBLIC + ACC_SUPER, byteCodeName( type ), signature( type ),
                byteCodeName( base ), iNames.length != 0 ? iNames : null );
        this.type = type;
        this.base = base;
    }

    @Override
    public MethodEmitter method( MethodDeclaration signature )
    {
        return new MethodByteCodeEmitter( classWriter, signature, base );
    }

    @Override
    public void field( FieldReference field, Expression value )
    {
        //keep track of all static field->value, and initiate in <clinit> in done
        if ( Modifier.isStatic( field.modifiers() ) )
        {
            staticFields.put( field, value );
        }
        FieldVisitor fieldVisitor = classWriter
                .visitField( field.modifiers(), field.name(), typeName( field.type() ), signature( field.type() ),
                        null );
        fieldVisitor.visitEnd();
    }

    @Override
    public void done()
    {
        if ( !staticFields.isEmpty() )
        {
            MethodVisitor methodVisitor = classWriter.visitMethod( ACC_STATIC, "<clinit>", "()V", null, null );
            ByteCodeExpressionVisitor expressionVisitor = new ByteCodeExpressionVisitor( methodVisitor );
            methodVisitor.visitCode();
            for ( Map.Entry<FieldReference,Expression> entry : staticFields.entrySet() )
            {
                FieldReference field = entry.getKey();
                entry.getValue().accept( expressionVisitor );
                methodVisitor.visitFieldInsn( PUTSTATIC, byteCodeName( field.owner() ),
                        field.name(), signature( field.type() ) );
            }
            methodVisitor.visitInsn( RETURN );
            methodVisitor.visitMaxs( 0, 0 );
            methodVisitor.visitEnd();
        }
        classWriter.visitEnd();
    }

    public ByteCodes toByteCodes()
    {
        return new ByteCodes()
        {
            @Override
            public String name()
            {
                return type.name();
            }

            @Override
            public ByteBuffer bytes()
            {
                return ByteBuffer.wrap( classWriter.toByteArray() );
            }
        };
    }

    private static class MethodByteCodeEmitter implements MethodEmitter
    {
        private final MethodVisitor methodVisitor;
        private final MethodDeclaration declaration;
        private final ByteCodeExpressionVisitor expressionVisitor;
        private boolean calledSuper = false;
        private final TypeReference base;

        public MethodByteCodeEmitter( ClassWriter classWriter, MethodDeclaration declaration, TypeReference base )
        {
            this.declaration = declaration;
            this.base = base;
            this.methodVisitor = classWriter.visitMethod( ACC_PUBLIC, declaration.name(), desc( declaration ),
                    signature( declaration ), exceptions( declaration ) );
            this.methodVisitor.visitCode();
            this.expressionVisitor = new ByteCodeExpressionVisitor( this.methodVisitor );
        }

        @Override
        public void done()
        {
            callSuperIfNecessary( );

            if ( declaration.returnType().isVoid() )
            {
                methodVisitor.visitInsn( RETURN );
            }
            //we rely on asm to keep track of stack depth
            methodVisitor.visitMaxs( 0, 0 );
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
            callSuperIfNecessary( );

            target.accept( expressionVisitor );
            value.accept( expressionVisitor );
            methodVisitor
                    .visitFieldInsn( PUTFIELD, byteCodeName( field.owner() ), field.name(), typeName( field.type() ) );

        }

        @Override
        public void returns()
        {
            callSuperIfNecessary( );

            methodVisitor.visitInsn( RETURN );
        }

        @Override
        public void returns( Expression value )
        {
            callSuperIfNecessary( );

            value.accept( expressionVisitor );
            switch ( declaration.returnType().simpleName() )
            {
            case "int":
            case "byte":
            case "short":
            case "char":
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
        public void assign( TypeReference type, String name, Expression value )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void beginWhile( Expression test )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void beginIf( Expression test )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void beginFinally()
        {
            callSuperIfNecessary( );
        }

        @Override
        public void endBlock()
        {
            callSuperIfNecessary( );
        }

        @Override
        public void beginTry( Resource... resources )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void throwException( Expression exception )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void beginCatch( Parameter exception )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void declare( LocalVariable local )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void assign( LocalVariable local, Expression value )
        {
            callSuperIfNecessary( );
        }

        @Override
        public void beginForEach( Parameter local, Expression iterable )
        {
            callSuperIfNecessary( );
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

    private static class ByteCodeExpressionVisitor implements ExpressionVisitor
    {
        private final MethodVisitor methodVisitor;

        private ByteCodeExpressionVisitor( MethodVisitor methodVisitor )
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
            methodVisitor.visitMethodInsn( INVOKESPECIAL, byteCodeName( method.owner() ), method.name(), desc( method ),
                    false );
        }

        @Override
        public void invoke( MethodReference method, Expression[] arguments )
        {

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
                methodVisitor.visitVarInsn( ILOAD, variable.index() );
                break;
            case "long":
                methodVisitor.visitVarInsn( LLOAD, variable.index() );
                break;
            case "float":
                methodVisitor.visitVarInsn( FLOAD, variable.index() );
                break;
            case "double":
                methodVisitor.visitVarInsn( DLOAD, variable.index() );
                break;
            default:
                methodVisitor.visitVarInsn( ALOAD, variable.index() );
            }
        }

        @Override
        public void getField( Expression target, FieldReference field )
        {
            target.accept( this );
            methodVisitor
                    .visitFieldInsn( GETFIELD, byteCodeName( field.owner() ), field.name(), typeName( field.type() ) );

        }

        @Override
        public void constant( Object value )
        {

        }

        @Override
        public void getStatic( FieldReference field )
        {

        }

        @Override
        public void loadThis( String sourceName )
        {
            methodVisitor.visitVarInsn( ALOAD, 0 );
        }

        @Override
        public void newInstance( TypeReference type )
        {

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

        }
    }

}
