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
import static org.objectweb.asm.Opcodes.AASTORE;
import static org.objectweb.asm.Opcodes.ACONST_NULL;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ANEWARRAY;
import static org.objectweb.asm.Opcodes.BASTORE;
import static org.objectweb.asm.Opcodes.BIPUSH;
import static org.objectweb.asm.Opcodes.CASTORE;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.DADD;
import static org.objectweb.asm.Opcodes.DASTORE;
import static org.objectweb.asm.Opcodes.DCMPG;
import static org.objectweb.asm.Opcodes.DCMPL;
import static org.objectweb.asm.Opcodes.DLOAD;
import static org.objectweb.asm.Opcodes.DMUL;
import static org.objectweb.asm.Opcodes.DSUB;
import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.FADD;
import static org.objectweb.asm.Opcodes.FASTORE;
import static org.objectweb.asm.Opcodes.FCMPG;
import static org.objectweb.asm.Opcodes.FCMPL;
import static org.objectweb.asm.Opcodes.FLOAD;
import static org.objectweb.asm.Opcodes.FMUL;
import static org.objectweb.asm.Opcodes.FSUB;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.IADD;
import static org.objectweb.asm.Opcodes.IASTORE;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.ICONST_1;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IFGE;
import static org.objectweb.asm.Opcodes.IFGT;
import static org.objectweb.asm.Opcodes.IFLE;
import static org.objectweb.asm.Opcodes.IFLT;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.IFNONNULL;
import static org.objectweb.asm.Opcodes.IFNULL;
import static org.objectweb.asm.Opcodes.IF_ACMPEQ;
import static org.objectweb.asm.Opcodes.IF_ACMPNE;
import static org.objectweb.asm.Opcodes.IF_ICMPEQ;
import static org.objectweb.asm.Opcodes.IF_ICMPGE;
import static org.objectweb.asm.Opcodes.IF_ICMPGT;
import static org.objectweb.asm.Opcodes.IF_ICMPLE;
import static org.objectweb.asm.Opcodes.IF_ICMPLT;
import static org.objectweb.asm.Opcodes.IF_ICMPNE;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.IMUL;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.ISUB;
import static org.objectweb.asm.Opcodes.L2D;
import static org.objectweb.asm.Opcodes.LADD;
import static org.objectweb.asm.Opcodes.LASTORE;
import static org.objectweb.asm.Opcodes.LCMP;
import static org.objectweb.asm.Opcodes.LCONST_0;
import static org.objectweb.asm.Opcodes.LCONST_1;
import static org.objectweb.asm.Opcodes.LLOAD;
import static org.objectweb.asm.Opcodes.LMUL;
import static org.objectweb.asm.Opcodes.LSUB;
import static org.objectweb.asm.Opcodes.NEW;
import static org.objectweb.asm.Opcodes.NEWARRAY;
import static org.objectweb.asm.Opcodes.POP;
import static org.objectweb.asm.Opcodes.POP2;
import static org.objectweb.asm.Opcodes.SASTORE;
import static org.objectweb.asm.Opcodes.SIPUSH;
import static org.objectweb.asm.Opcodes.T_BOOLEAN;
import static org.objectweb.asm.Opcodes.T_BYTE;
import static org.objectweb.asm.Opcodes.T_CHAR;
import static org.objectweb.asm.Opcodes.T_DOUBLE;
import static org.objectweb.asm.Opcodes.T_FLOAT;
import static org.objectweb.asm.Opcodes.T_INT;
import static org.objectweb.asm.Opcodes.T_LONG;
import static org.objectweb.asm.Opcodes.T_SHORT;

class ByteCodeExpressionVisitor implements ExpressionVisitor
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
                    .visitMethodInsn( INVOKEINTERFACE, byteCodeName( method.owner() ), method.name(),
                            desc( method ),
                            true );
        }
        else if ( method.isConstructor() )
        {
            methodVisitor
                    .visitMethodInsn( INVOKESPECIAL, byteCodeName( method.owner() ), method.name(), desc( method ),
                            false );
        }
        else
        {
            methodVisitor
                    .visitMethodInsn( INVOKEVIRTUAL, byteCodeName( method.owner() ), method.name(), desc( method ),
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
        methodVisitor.visitMethodInsn( INVOKESTATIC,
                byteCodeName( method.owner() ),
                method.name(), desc( method ), Modifier.isInterface( method.owner().modifiers() ) );
    }

    @Override
    public void load( LocalVariable variable )
    {
        if ( variable.type().isPrimitive() )
        {
            switch ( variable.type().name() )
            {
            case "int":
            case "byte":
            case "short":
            case "char":
            case "boolean":
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
        else
        {
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
        if ( value == null )
        {
            methodVisitor.visitInsn( ACONST_NULL );
        }
        else if ( value instanceof Integer )
        {
            pushInteger( (Integer) value );
        }
        else if ( value instanceof Byte )
        {
            pushInteger( (Byte) value );
        }
        else if ( value instanceof Short )
        {
            pushInteger( (Short) value );
        }
        else if ( value instanceof Long )
        {
            pushLong( (Long) value );
        }
        else if ( value instanceof Double )
        {
            methodVisitor.visitLdcInsn( value );
        }
        else if ( value instanceof Float )
        {
            methodVisitor.visitLdcInsn( value );
        }
        else if ( value instanceof Boolean )
        {
            boolean b = (boolean) value;
            methodVisitor.visitInsn( b ? ICONST_1 : ICONST_0 );
        }
        else
        {
            methodVisitor.visitLdcInsn( value );
        }
    }

    @Override
    public void getStatic( FieldReference field )
    {
        methodVisitor
                .visitFieldInsn( GETSTATIC, byteCodeName( field.owner() ), field.name(), typeName( field.type() ) );
    }

    @Override
    public void loadThis( String sourceName )
    {
        methodVisitor.visitVarInsn( ALOAD, 0 );
    }

    @Override
    public void newInstance( TypeReference type )
    {
        methodVisitor.visitTypeInsn( NEW, byteCodeName( type ) );
        methodVisitor.visitInsn( DUP );
    }

    @Override
    public void not( Expression expression )
    {
        test( IFNE, expression, Expression.TRUE, Expression.FALSE );
    }

    @Override
    public void isNull( Expression expression )
    {
        test( IFNONNULL, expression, Expression.TRUE, Expression.FALSE );
    }

    @Override
    public void notNull( Expression expression )
    {
        test( IFNULL, expression, Expression.TRUE, Expression.FALSE );
    }

    @Override
    public void ternary( Expression test, Expression onTrue, Expression onFalse )
    {
        test( IFEQ, test, onTrue, onFalse );
    }

    public void ternaryOnNull( Expression test, Expression onTrue, Expression onFalse )
    {
        test( IFNONNULL, test, onTrue, onFalse );
    }

    public void ternaryOnNonNull( Expression test, Expression onTrue, Expression onFalse )
    {
        test( IFNULL, test, onTrue, onFalse );
    }

    private void test( int test, Expression predicate, Expression onTrue, Expression onFalse )
    {
        predicate.accept( this );
        Label isFalse = new Label();
        methodVisitor.visitJumpInsn( test, isFalse );
        onTrue.accept( this );
        Label after = new Label();
        methodVisitor.visitJumpInsn( GOTO, after );
        methodVisitor.visitLabel( isFalse );
        onFalse.accept( this );
        methodVisitor.visitLabel( after );
    }

    @Override
    public void equal( Expression lhs, Expression rhs )
    {
        equal( lhs, rhs, true );
    }

    @Override
    public void notEqual( Expression lhs, Expression rhs )
    {
        equal( lhs, rhs, false );
    }

    private void equal( Expression lhs, Expression rhs, boolean equal )
    {
        if ( lhs.type().isPrimitive() )
        {
            assert rhs.type().isPrimitive();

            switch ( lhs.type().name() )
            {
            case "int":
            case "byte":
            case "short":
            case "char":
            case "boolean":
                assertSameType( lhs, rhs, "compare" );
                compareIntOrReferenceType( lhs, rhs, equal ? IF_ICMPNE : IF_ICMPEQ );
                break;
            case "long":
                assertSameType( lhs, rhs, "compare" );
                compareLongOrFloatType( lhs, rhs, LCMP, equal ? IFNE : IFEQ );
                break;
            case "float":
                assertSameType( lhs, rhs, "compare" );
                compareLongOrFloatType( lhs, rhs, FCMPL, equal ? IFNE : IFEQ );
                break;
            case "double":
                assertSameType( lhs, rhs, "compare" );
                compareLongOrFloatType( lhs, rhs, DCMPL, equal ? IFNE : IFEQ );
                break;
            default:
                compareIntOrReferenceType( lhs, rhs, equal ? IF_ACMPNE : IF_ACMPEQ );
            }
        }
        else
        {
            assert !(rhs.type().isPrimitive());
            compareIntOrReferenceType( lhs, rhs, equal ? IF_ACMPNE : IF_ACMPEQ );
        }
    }

    @Override
    public void or( Expression... expressions )
    {
        assert expressions.length >= 2;
        /*
         * something like:
         *
         * LOAD expression1
         * IF TRUE GOTO 0
         * LOAD expression2
         * IF TRUE GOTO 0
         * ...
         * LOAD expressionN
         * IF FALSE GOTO 1
         * 0: // The reason we have this extra block for the true case is because we mimic what javac does
         *    // hoping that it will be nice to the JIT compiler
         *  LOAD TRUE
         *  GOTO 2
         * 1:
         *  LOAD FALSE
         * 2:
         *  ...continue doing stuff
         */
        Label l0 = new Label();
        Label l1 = new Label();
        Label l2 = new Label();
        for ( int i = 0; i < expressions.length; i++ )
        {
            expressions[i].accept( this );
            if ( i < expressions.length - 1 )
            {
                methodVisitor.visitJumpInsn( IFNE, l0 );
            }
        }
        methodVisitor.visitJumpInsn( IFEQ, l1 );
        methodVisitor.visitLabel( l0 );
        methodVisitor.visitInsn( ICONST_1 );
        methodVisitor.visitJumpInsn( GOTO, l2 );
        methodVisitor.visitLabel( l1 );
        methodVisitor.visitInsn( ICONST_0 );
        methodVisitor.visitLabel( l2 );
    }

    @Override
    public void and( Expression... expressions )
    {
        assert expressions.length >= 2;
        /*
         * something like:
         *
         * LOAD expression1
         * IF FALSE GOTO 0
         * LOAD expression2
         * IF FALSE GOTO 0
         * LOAD TRUE
         * ...
         * LOAD expressionN
         * IF FALSE GOTO 0
         * GOTO 1
         * 0:
         *  LOAD FALSE
         * 1:
         *  ...continue doing stuff
         */
        Label l0 = new Label();
        Label l1 = new Label();
        for ( Expression expression : expressions )
        {
            expression.accept( this );
            methodVisitor.visitJumpInsn( IFEQ, l0 );
        }
        methodVisitor.visitInsn( ICONST_1 );
        methodVisitor.visitJumpInsn( GOTO, l1 );
        methodVisitor.visitLabel( l0 );
        methodVisitor.visitInsn( ICONST_0 );
        methodVisitor.visitLabel( l1 );
    }

    @Override
    public void add( Expression lhs, Expression rhs )
    {
        assertSameType( lhs, rhs, "add" );
        lhs.accept( this );
        rhs.accept( this );

        numberOperation( lhs.type(),
                () -> methodVisitor.visitInsn( IADD ),
                () -> methodVisitor.visitInsn( LADD ),
                () -> methodVisitor.visitInsn( FADD ),
                () -> methodVisitor.visitInsn( DADD ) );
    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {
        assertSameType( lhs, rhs, "compare" );
        numberOperation( lhs.type(),
                () -> compareIntOrReferenceType( lhs, rhs, IF_ICMPLE ),
                () -> compareLongOrFloatType( lhs, rhs, LCMP, IFLE ),
                () -> compareLongOrFloatType( lhs, rhs, FCMPL, IFLE ),
                () -> compareLongOrFloatType( lhs, rhs, DCMPL, IFLE )
        );
    }

    @Override
    public void gte( Expression lhs, Expression rhs )
    {
        assertSameType( lhs, rhs, "compare" );
        numberOperation( lhs.type(),
                () -> compareIntOrReferenceType( lhs, rhs, IF_ICMPLT ),
                () -> compareLongOrFloatType( lhs, rhs, LCMP, IFLT ),
                () -> compareLongOrFloatType( lhs, rhs, FCMPL, IFLT ),
                () -> compareLongOrFloatType( lhs, rhs, DCMPL, IFLT )
        );
    }

    @Override
    public void lt( Expression lhs, Expression rhs )
    {
        assertSameType( lhs, rhs, "compare" );
        numberOperation( lhs.type(),
                () -> compareIntOrReferenceType( lhs, rhs, IF_ICMPGE ),
                () -> compareLongOrFloatType( lhs, rhs, LCMP, IFGE ),
                () -> compareLongOrFloatType( lhs, rhs, FCMPG, IFGE ),
                () -> compareLongOrFloatType( lhs, rhs, DCMPG, IFGE )
        );
    }

    @Override
    public void lte( Expression lhs, Expression rhs )
    {
        assertSameType( lhs, rhs, "compare" );
        numberOperation( lhs.type(),
                () -> compareIntOrReferenceType( lhs, rhs, IF_ICMPGT ),
                () -> compareLongOrFloatType( lhs, rhs, LCMP, IFGT ),
                () -> compareLongOrFloatType( lhs, rhs, FCMPG, IFGT ),
                () -> compareLongOrFloatType( lhs, rhs, DCMPG, IFGT )
        );
    }

    @Override
    public void subtract( Expression lhs, Expression rhs )
    {
        assertSameType( lhs, rhs, "subtract" );
        lhs.accept( this );
        rhs.accept( this );
        numberOperation( lhs.type(),
                () -> methodVisitor.visitInsn( ISUB ),
                () -> methodVisitor.visitInsn( LSUB ),
                () -> methodVisitor.visitInsn( FSUB ),
                () -> methodVisitor.visitInsn( DSUB ) );
    }

    @Override
    public void multiply( Expression lhs, Expression rhs )
    {
        assertSameType( lhs, rhs, "multiply" );
        lhs.accept( this );
        rhs.accept( this );
        numberOperation( lhs.type(),
                () -> methodVisitor.visitInsn( IMUL ),
                () -> methodVisitor.visitInsn( LMUL ),
                () -> methodVisitor.visitInsn( FMUL ),
                () -> methodVisitor.visitInsn( DMUL ) );
    }

    @Override
    public void cast( TypeReference type, Expression expression )
    {
        expression.accept( this );
        if ( !type.equals( expression.type() ) )
        {
            methodVisitor.visitTypeInsn( CHECKCAST, byteCodeName( type ) );
        }
    }

    @Override
    public void newArray( TypeReference type, Expression... exprs )
    {
        pushInteger( exprs.length );
        createArray( type );
        for ( int i = 0; i < exprs.length; i++ )
        {
            methodVisitor.visitInsn( DUP );
            pushInteger( i );
            exprs[i].accept( this );
            arrayStore( type );
        }
    }

    @Override
    public void longToDouble( Expression expression )
    {
        expression.accept( this );
        methodVisitor.visitInsn( L2D );
    }

    @Override
    public void pop( Expression expression )
    {
        expression.accept( this );
        switch ( expression.type().simpleName() )
        {
        case "long":
        case "double":
            methodVisitor.visitInsn( POP2 );
            break;
        default:
            methodVisitor.visitInsn( POP );
            break;
        }
    }

    @Override
    public void box( Expression expression )
    {
        expression.accept( this );
        if ( expression.type().isPrimitive() )
        {
            switch ( expression.type().name() )
            {
            case "byte":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false );
                break;
            case "short":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false );
                break;
            case "int":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;", false );
                break;
            case "long":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false );
                break;
            case "char":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Character", "valueOf", "(C)Ljava/lang/Character;", false );
                break;
            case "boolean":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false );
                break;
            case "float":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false );
                break;
            case "double":
                methodVisitor.visitMethodInsn( INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;", false );
                break;
            default:
                //do nothing, expression is already boxed
            }
        }
    }

    @Override
    public void unbox( Expression expression )
    {
        expression.accept( this );
        switch ( expression.type().fullName() )
        {
        case "java.lang.Byte":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Byte", "byteValue", "()B", false);
            break;
        case "java.lang.Short":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Short", "shortValue", "()S", false);
            break;
        case "java.lang.Integer":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
            break;
        case "java.lang.Long":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Long", "longValue", "()J", false);
            break;
        case "java.lang.Character":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Character", "charValue", "()C", false);
            break;
        case "java.lang.Boolean":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z", false);
            break;
        case "java.lang.Float":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Float", "floatValue", "()F", false);
            break;
        case "java.lang.Double":
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Double", "doubleValue", "()D", false);
            break;
        default:
            throw new IllegalStateException( "Cannot unbox " + expression.type().fullName() );
        }
    }

    private void compareIntOrReferenceType( Expression lhs, Expression rhs, int opcode )
    {
        lhs.accept( this );
        rhs.accept( this );

        Label l0 = new Label();
        methodVisitor.visitJumpInsn( opcode, l0 );
        methodVisitor.visitInsn( ICONST_1 );
        Label l1 = new Label();
        methodVisitor.visitJumpInsn( GOTO, l1 );
        methodVisitor.visitLabel( l0 );
        methodVisitor.visitInsn( ICONST_0 );
        methodVisitor.visitLabel( l1 );
    }

    private void compareLongOrFloatType( Expression lhs, Expression rhs, int opcode, int compare )
    {
        lhs.accept( this );
        rhs.accept( this );

        methodVisitor.visitInsn( opcode );
        Label l0 = new Label();
        methodVisitor.visitJumpInsn( compare, l0 );
        methodVisitor.visitInsn( ICONST_1 );
        Label l1 = new Label();
        methodVisitor.visitJumpInsn( GOTO, l1 );
        methodVisitor.visitLabel( l0 );
        methodVisitor.visitInsn( ICONST_0 );
        methodVisitor.visitLabel( l1 );
    }

    private void pushInteger( int integer )
    {
        if ( integer < 6 && integer >= -1 )
        {
            //LOAD fast, specialized constant instructions
            //ICONST_M1 = 2;
            //ICONST_0 = 3;
            //ICONST_1 = 4;
            //ICONST_2 = 5;
            //ICONST_3 = 6;
            //ICONST_4 = 7;
            //ICONST_5 = 8;
            methodVisitor.visitInsn( ICONST_0 + integer );
        }
        else if ( integer < Byte.MAX_VALUE && integer > Byte.MIN_VALUE )
        {
            methodVisitor.visitIntInsn( BIPUSH, integer );
        }
        else if ( integer < Short.MAX_VALUE && integer > Short.MIN_VALUE )
        {
            methodVisitor.visitIntInsn( SIPUSH, integer );
        }
        else
        {
            methodVisitor.visitLdcInsn( integer );
        }
    }

    private void pushLong( long integer )
    {
        if ( integer == 0L )
        {
            methodVisitor.visitInsn( LCONST_0 );
        }
        else if ( integer == 1L )
        {
            methodVisitor.visitInsn( LCONST_1 );
        }
        else
        {
            methodVisitor.visitLdcInsn( integer );
        }
    }

    private void createArray( TypeReference reference )
    {
        if ( reference.isPrimitive() )
        {
            switch ( reference.name() )
            {
            case "int":
                methodVisitor.visitIntInsn( NEWARRAY, T_INT );
                break;
            case "long":
                methodVisitor.visitIntInsn( NEWARRAY, T_LONG );
                break;
            case "byte":
                methodVisitor.visitIntInsn( NEWARRAY, T_BYTE );
                break;
            case "short":
                methodVisitor.visitIntInsn( NEWARRAY, T_SHORT );
                break;
            case "char":
                methodVisitor.visitIntInsn( NEWARRAY, T_CHAR );
                break;
            case "float":
                methodVisitor.visitIntInsn( NEWARRAY, T_FLOAT );
                break;
            case "double":
                methodVisitor.visitIntInsn( NEWARRAY, T_DOUBLE );
                break;
            case "boolean":
                methodVisitor.visitIntInsn( NEWARRAY, T_BOOLEAN );
                break;
            default:
                methodVisitor.visitTypeInsn( ANEWARRAY, byteCodeName( reference ) );
            }
        }
        else
        {
            methodVisitor.visitTypeInsn( ANEWARRAY, byteCodeName( reference ) );
        }
    }

    private void arrayStore( TypeReference reference )
    {
        if ( reference.isPrimitive() )
        {
            switch ( reference.name() )
            {
            case "int":
                methodVisitor.visitInsn( IASTORE );
                break;
            case "long":
                methodVisitor.visitInsn( LASTORE );
                break;
            case "byte":
                methodVisitor.visitInsn( BASTORE );
                break;
            case "short":
                methodVisitor.visitInsn( SASTORE );
                break;
            case "char":
                methodVisitor.visitInsn( CASTORE );
                break;
            case "float":
                methodVisitor.visitInsn( FASTORE );
                break;
            case "double":
                methodVisitor.visitInsn( DASTORE );
                break;
            case "boolean":
                methodVisitor.visitInsn( BASTORE );
                break;
            default:
                methodVisitor.visitInsn( AASTORE );
            }
        }
        else
        {
            methodVisitor.visitInsn( AASTORE );
        }
    }

    private void numberOperation( TypeReference type, Runnable onInt, Runnable onLong, Runnable onFloat,
            Runnable onDouble )
    {
        if ( !type.isPrimitive() )
        {
            throw new IllegalStateException( "Cannot compare reference types" );
        }

        switch ( type.name() )
        {
        case "int":
        case "byte":
        case "short":
        case "char":
        case "boolean":
            onInt.run();
            break;
        case "long":
            onLong.run();
            break;
        case "float":
            onFloat.run();
            break;
        case "double":
            onDouble.run();
            break;
        default:
            throw new IllegalStateException( "Cannot compare reference types" );
        }
    }

    private void assertSameType( Expression lhs, Expression rhs, String operation )
    {
        if ( !lhs.type().equals( rhs.type() ) )
        {
            throw new IllegalArgumentException(
                    String.format( "Can only %s values of the same type (lhs: %s, rhs: %s)", operation, lhs.type().toString(), rhs.type().toString() )
            );
        }
    }

}
