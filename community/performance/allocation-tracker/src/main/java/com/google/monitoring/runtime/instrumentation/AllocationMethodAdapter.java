/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.google.monitoring.runtime.instrumentation;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.LocalVariablesSorter;

import java.util.LinkedList;
import java.util.List;

/**
 * A <code>MethodAdapter</code> that instruments all heap allocation bytecodes
 * to record the allocation being done for profiling.
 * Instruments bytecodes that allocate heap memory to call a recording hook.
 *
 * @author Ami Fischman
 */
class AllocationMethodAdapter extends MethodVisitor
{
    /**
     * The signature string the recorder method must have.  The method must be
     * static, return void, and take as arguments:
     * <ol>
     * <li>an int count of how many instances are being allocated.  -1 means a
     * simple new to distinguish from a 1-element array.  0 shows up as a value
     * here sometimes; one reason is toArray()-type methods that require an array
     * type argument (see ArrayList.toArray() for example).</li>
     * <li>a String descriptor of the class/primitive type being allocated.</li>
     * <li>an Object reference to the just-allocated Object.</li>
     * </ol>
     */
    public static final String RECORDER_SIGNATURE =
            "(ILjava/lang/String;Ljava/lang/Object;)V";

    /**
     * Like RECORDER_SIGNATURE, but for a method that extracts all of
     * the information dynamically from a class.
     */
    public static final String CLASS_RECORDER_SIG =
            "(Ljava/lang/Class;Ljava/lang/Object;)V";

    // A helper struct for describing the scope of temporary local variables we
    // create as part of the instrumentation.
    private static class VariableScope
    {
        public final int index;
        public final Label start;
        public final Label end;
        public final String desc;

        public VariableScope( int index, Label start, Label end, String desc )
        {
            this.index = index;
            this.start = start;
            this.end = end;
            this.desc = desc;
        }
    }

    // Dictionary of primitive type opcode to English name.
    private static final String[] primitiveTypeNames = new String[]{
            "INVALID0", "INVALID1", "INVALID2", "INVALID3",
            "boolean", "char", "float", "double",
            "byte", "short", "int", "long"
    };

    // To track the difference between <init>'s called as the result of a NEW
    // and <init>'s called because of superclass initialization, we track the
    // number of NEWs that still need to have their <init>'s called.
    private int outstandingAllocs = 0;

    // We need to set the scope of any local variables we materialize;
    // accumulate the scopes here and set them all at the end of the visit to
    // ensure all labels have been resolved.  Allocated on-demand.
    private List<VariableScope> localScopes = null;

    private List<VariableScope> getLocalScopes()
    {
        if ( localScopes == null )
        {
            localScopes = new LinkedList<VariableScope>();
        }
        return localScopes;
    }

    private final String recorderClass;
    private final String recorderMethod;

    /**
     * The LocalVariablesSorter used in this adapter.  Lame that it's public but
     * the ASM architecture requires setting it from the outside after this
     * AllocationMethodAdapter is fully constructed and the LocalVariablesSorter
     * constructor requires a reference to this adapter.  The only setter of
     * this should be AllocationClassAdapter.visitMethod().
     */
    public LocalVariablesSorter lvs = null;

    /**
     * A new AllocationMethodAdapter is created for each method that gets visited.
     */
    public AllocationMethodAdapter( MethodVisitor mv, String recorderClass,
                                    String recorderMethod )
    {
        super( Opcodes.ASM4, mv );
        this.recorderClass = recorderClass;
        this.recorderMethod = recorderMethod;
    }

    /**
     * newarray shows up as an instruction taking an int operand (the primitive
     * element type of the array) so we hook it here.
     */
    @Override
    public void visitIntInsn( int opcode, int operand )
    {
        if ( opcode == Opcodes.NEWARRAY )
        {
            // instack: ... count
            // outstack: ... aref
            if ( operand >= 4 && operand <= 11 )
            {
                super.visitInsn( Opcodes.DUP ); // -> stack: ... count count
                super.visitIntInsn( opcode, operand ); // -> stack: ... count aref
                invokeRecordAllocation( primitiveTypeNames[operand] );
                // -> stack: ... aref
            }
            else
            {
                AllocationInstrumenter.logger.severe( "NEWARRAY called with an invalid operand " +
                        operand + ".  Not instrumenting this allocation!" );
                super.visitIntInsn( opcode, operand );
            }
        }
        else
        {
            super.visitIntInsn( opcode, operand );
        }
    }

    // Helper method to compute class name as a String and push it on the stack.
    // pre: stack: ... class
    // post: stack: ... class className
    private void pushClassNameOnStack()
    {
        super.visitInsn( Opcodes.DUP );
        // -> stack: ... class class
        super.visitMethodInsn( Opcodes.INVOKEVIRTUAL, "java/lang/Class",
                "getName", "()Ljava/lang/String;" );
        // -> stack: ... class classNameDotted
        super.visitLdcInsn( '.' );
        // -> stack: ... class classNameDotted '.'
        super.visitLdcInsn( '/' );
        // -> stack: ... class classNameDotted '.' '/'
        super.visitMethodInsn( Opcodes.INVOKEVIRTUAL, "java/lang/String",
                "replace", "(CC)Ljava/lang/String;" );
        // -> stack: ... class className
    }

    // Helper method to compute the product of an integer array and push it on
    // the stack.
    // pre: stack: ... intArray
    // post: stack: ... intArray product
    private void pushProductOfIntArrayOnStack()
    {
        Label beginScopeLabel = new Label();
        Label endScopeLabel = new Label();

        int dimsArrayIndex = newLocal( "[I", beginScopeLabel, endScopeLabel );
        int counterIndex = newLocal( "I", beginScopeLabel, endScopeLabel );
        int productIndex = newLocal( "I", beginScopeLabel, endScopeLabel );
        Label loopLabel = new Label();
        Label endLabel = new Label();

        super.visitLabel( beginScopeLabel );

        // stack: ... intArray
        super.visitVarInsn( Opcodes.ASTORE, dimsArrayIndex );
        // -> stack: ...

        // counter = 0
        super.visitInsn( Opcodes.ICONST_0 );
        super.visitVarInsn( Opcodes.ISTORE, counterIndex );
        // product = 1
        super.visitInsn( Opcodes.ICONST_1 );
        super.visitVarInsn( Opcodes.ISTORE, productIndex );
        // loop:
        super.visitLabel( loopLabel );
        // if index >= arraylength goto end:
        super.visitVarInsn( Opcodes.ILOAD, counterIndex );
        super.visitVarInsn( Opcodes.ALOAD, dimsArrayIndex );
        super.visitInsn( Opcodes.ARRAYLENGTH );
        super.visitJumpInsn( Opcodes.IF_ICMPGE, endLabel );
        // product = product * max(array[counter],1)
        super.visitVarInsn( Opcodes.ALOAD, dimsArrayIndex );
        super.visitVarInsn( Opcodes.ILOAD, counterIndex );
        super.visitInsn( Opcodes.IALOAD );
        super.visitInsn( Opcodes.DUP );
        Label nonZeroDimension = new Label();
        super.visitJumpInsn( Opcodes.IFNE, nonZeroDimension );
        super.visitInsn( Opcodes.POP );
        super.visitInsn( Opcodes.ICONST_1 );
        super.visitLabel( nonZeroDimension );
        super.visitVarInsn( Opcodes.ILOAD, productIndex );
        super.visitInsn( Opcodes.IMUL ); // if overflow happens it happens.
        super.visitVarInsn( Opcodes.ISTORE, productIndex );
        // iinc counter 1
        super.visitIincInsn( counterIndex, 1 );
        // goto loop
        super.visitJumpInsn( Opcodes.GOTO, loopLabel );
        // end:
        super.visitLabel( endLabel );
        // re-push dimensions array
        super.visitVarInsn( Opcodes.ALOAD, dimsArrayIndex );
        // push product
        super.visitVarInsn( Opcodes.ILOAD, productIndex );

        super.visitLabel( endScopeLabel );
    }

    /**
     * Reflection-based allocation (@see java.lang.reflect.Array#newInstance) is
     * triggered with a static method call (INVOKESTATIC), so we hook it here.
     * Class initialization is triggered with a constructor call (INVOKESPECIAL)
     * so we hook that here too as a proxy for the new bytecode which leaves an
     * uninitialized object on the stack that we're not allowed to touch.
     * {@link java.lang.Object#clone} is also a call to INVOKESPECIAL,
     * and is hooked here.  {@link java.lang.Class#newInstance} and
     * {@link java.lang.reflect.Constructor#newInstance} are both
     * INVOKEVIRTUAL calls, so they are hooked here, as well.
     */
    @Override
    public void visitMethodInsn( int opcode, String owner, String name,
                                 String signature )
    {
        if ( opcode == Opcodes.INVOKESTATIC &&
                // Array does its own native allocation.  Grr.
                owner.equals( "java/lang/reflect/Array" ) &&
                name.equals( "newInstance" ) )
        {
            if ( signature.equals( "(Ljava/lang/Class;I)Ljava/lang/Object;" ) )
            {

                Label beginScopeLabel = new Label();
                Label endScopeLabel = new Label();
                super.visitLabel( beginScopeLabel );

                // stack: ... class count
                int countIndex = newLocal( "I", beginScopeLabel, endScopeLabel );
                super.visitVarInsn( Opcodes.ISTORE, countIndex );
                // -> stack: ... class
                pushClassNameOnStack();
                // -> stack: ... class className
                int typeNameIndex =
                        newLocal( "Ljava/lang/String;", beginScopeLabel, endScopeLabel );
                super.visitVarInsn( Opcodes.ASTORE, typeNameIndex );
                // -> stack: ... class
                super.visitVarInsn( Opcodes.ILOAD, countIndex );
                // -> stack: ... class count
                super.visitMethodInsn( opcode, owner, name, signature );
                // -> stack: ... newobj
                super.visitInsn( Opcodes.DUP );
                // -> stack: ... newobj newobj
                super.visitVarInsn( Opcodes.ILOAD, countIndex );
                // -> stack: ... newobj newobj count
                super.visitInsn( Opcodes.SWAP );
                // -> stack: ... newobj count newobj
                super.visitVarInsn( Opcodes.ALOAD, typeNameIndex );
                super.visitLabel( endScopeLabel );
                // -> stack: ... newobj count newobj className
                super.visitInsn( Opcodes.SWAP );
                // -> stack: ... newobj count className newobj
                super.visitMethodInsn( Opcodes.INVOKESTATIC, recorderClass,
                        recorderMethod, RECORDER_SIGNATURE );
                // -> stack: ... newobj
                return;
            }
            else if ( signature.equals( "(Ljava/lang/Class;[I)Ljava/lang/Object;" ) )
            {
                Label beginScopeLabel = new Label();
                Label endScopeLabel = new Label();
                super.visitLabel( beginScopeLabel );

                int dimsArrayIndex = newLocal( "[I", beginScopeLabel, endScopeLabel );
                // stack: ... class dimsArray
                pushProductOfIntArrayOnStack();
                // -> stack: ... class dimsArray product
                int productIndex = newLocal( "I", beginScopeLabel, endScopeLabel );
                super.visitVarInsn( Opcodes.ISTORE, productIndex );
                // -> stack: ... class dimsArray

                super.visitVarInsn( Opcodes.ASTORE, dimsArrayIndex );
                // -> stack: ... class
                pushClassNameOnStack();
                // -> stack: ... class className
                int typeNameIndex =
                        newLocal( "Ljava/lang/String;", beginScopeLabel, endScopeLabel );
                super.visitVarInsn( Opcodes.ASTORE, typeNameIndex );
                // -> stack: ... class
                super.visitVarInsn( Opcodes.ALOAD, dimsArrayIndex );
                // -> stack: ... class dimsArray
                super.visitMethodInsn( opcode, owner, name, signature );
                // -> stack: ... newobj

                super.visitInsn( Opcodes.DUP );
                // -> stack: ... newobj newobj
                super.visitVarInsn( Opcodes.ILOAD, productIndex );
                // -> stack: ... newobj newobj product
                super.visitInsn( Opcodes.SWAP );
                // -> stack: ... newobj product newobj
                super.visitVarInsn( Opcodes.ALOAD, typeNameIndex );
                super.visitLabel( endScopeLabel );
                // -> stack: ... newobj product newobj className
                super.visitInsn( Opcodes.SWAP );
                // -> stack: ... newobj product className newobj
                super.visitMethodInsn( Opcodes.INVOKESTATIC, recorderClass,
                        recorderMethod, RECORDER_SIGNATURE );
                // -> stack: ... newobj
                return;
            }
        }

        if ( opcode == Opcodes.INVOKEVIRTUAL )
        {
            if ( "clone".equals( name ) && owner.startsWith( "[" ) )
            {
                super.visitMethodInsn( opcode, owner, name, signature );

                int i = 0;
                while ( i < owner.length() )
                {
                    if ( owner.charAt( i ) != '[' )
                    {
                        break;
                    }
                    i++;
                }
                if ( i > 1 )
                {
                    // -> stack: ... newobj
                    super.visitTypeInsn( Opcodes.CHECKCAST, owner );
                    // -> stack: ... arrayref
                    calculateArrayLengthAndDispatch( owner.substring( i ), i );
                }
                else
                {
                    // -> stack: ... newobj
                    super.visitInsn( Opcodes.DUP );
                    // -> stack: ... newobj newobj
                    super.visitTypeInsn( Opcodes.CHECKCAST, owner );
                    // -> stack: ... newobj arrayref
                    super.visitInsn( Opcodes.ARRAYLENGTH );
                    // -> stack: ... newobj length
                    super.visitInsn( Opcodes.SWAP );
                    // -> stack: ... length newobj
                    invokeRecordAllocation( owner.substring( i ) );
                }
                return;
            }
            else if ( "newInstance".equals( name ) )
            {
                if ( "java/lang/Class".equals( owner ) &&
                        "()Ljava/lang/Object;".equals( signature ) )
                {
                    super.visitInsn( Opcodes.DUP );
                    // -> stack: ... Class Class
                    super.visitMethodInsn( opcode, owner, name, signature );
                    // -> stack: ... Class newobj
                    super.visitInsn( Opcodes.DUP_X1 );
                    // -> stack: ... newobj Class newobj
                    super.visitMethodInsn( Opcodes.INVOKESTATIC,
                            recorderClass, recorderMethod,
                            CLASS_RECORDER_SIG );
                    // -> stack: ... newobj
                    return;
                }
                else if ( "java/lang/reflect/Constructor".equals( owner ) &&
                        "([Ljava/lang/Object;)Ljava/lang/Object;".equals( signature ) )
                {
                    buildRecorderFromObject( opcode, owner, name, signature );
                    return;
                }
            }
        }

        if ( opcode == Opcodes.INVOKESPECIAL )
        {
            if ( "clone".equals( name ) && "java/lang/Object".equals( owner ) )
            {
                buildRecorderFromObject( opcode, owner, name, signature );
                return;
            }
            else if ( "<init>".equals( name ) && outstandingAllocs > 0 )
            {
                // Tricky because superclass initializers mean there can be more calls
                // to <init> than calls to NEW; hence outstandingAllocs.
                --outstandingAllocs;

                // Most of the time (i.e. in bytecode generated by javac) it is the case
                // that following an <init> call the top of the stack has a reference ot
                // the newly-initialized object.  But nothing in the JVM Spec requires
                // this, so we need to play games with the stack to make an explicit
                // extra copy (and then discard it).

                dupStackElementBeforeSignatureArgs( signature );
                super.visitMethodInsn( opcode, owner, name, signature );
                super.visitLdcInsn( -1 );
                super.visitInsn( Opcodes.SWAP );
                invokeRecordAllocation( owner );
                super.visitInsn( Opcodes.POP );
                return;
            }
        }

        super.visitMethodInsn( opcode, owner, name, signature );
    }

    // This is the instrumentation that occurs when there is no static
    // information about the class we are instantiating.  First we build the
    // object, then we get the class and invoke the recorder.
    private void buildRecorderFromObject(
            int opcode, String owner, String name, String signature )
    {
        super.visitMethodInsn( opcode, owner, name, signature );
        // -> stack: ... newobj
        super.visitInsn( Opcodes.DUP );
        // -> stack: ... newobj newobj
        super.visitInsn( Opcodes.DUP );
        // -> stack: ... newobj newobj newobj
        // We could be instantiating this class or a subclass, so we
        // have to get the class the hard way.
        super.visitMethodInsn( Opcodes.INVOKEVIRTUAL,
                "java/lang/Object",
                "getClass",
                "()Ljava/lang/Class;" );
        // -> stack: ... newobj newobj Class
        super.visitInsn( Opcodes.SWAP );
        // -> stack: ... newobj Class newobj
        super.visitMethodInsn( Opcodes.INVOKESTATIC,
                recorderClass, recorderMethod,
                CLASS_RECORDER_SIG );
        // -> stack: ... newobj
    }

    // Given a method signature interpret the top of the stack as the arguments
    // to the method, dup the top-most element preceding these arguments, and
    // leave the arguments alone.  This is done by inspecting each parameter
    // type, popping off the stack elements using the type information,
    // duplicating the target element, and pushing the arguments back on the
    // stack.
    private void dupStackElementBeforeSignatureArgs( final String sig )
    {
        final Label beginScopeLabel = new Label();
        final Label endScopeLabel = new Label();
        super.visitLabel( beginScopeLabel );

        Type[] argTypes = Type.getArgumentTypes( sig );
        int[] args = new int[argTypes.length];

        for ( int i = argTypes.length - 1; i >= 0; --i )
        {
            args[i] = newLocal( argTypes[i], beginScopeLabel, endScopeLabel );
            super.visitVarInsn( argTypes[i].getOpcode( Opcodes.ISTORE ), args[i] );
        }
        super.visitInsn( Opcodes.DUP );
        for ( int i = 0; i < argTypes.length; ++i )
        {
            super.visitVarInsn( argTypes[i].getOpcode( Opcodes.ILOAD ), args[i] );
        }
        super.visitLabel( endScopeLabel );
    }

    /**
     * new and anewarray bytecodes take a String operand for the type of
     * the object or array element so we hook them here.  Note that new doesn't
     * actually result in any instrumentation here; we just do a bit of
     * book-keeping and do the instrumentation following the constructor call
     * (because we're not allowed to touch the object until it is initialized).
     */
    @Override
    public void visitTypeInsn( int opcode, String typeName )
    {
        if ( opcode == Opcodes.NEW )
        {
            // We can't actually tag this object right after allocation because it
            // must be initialized with a ctor before we can touch it (Verifier
            // enforces this).  Instead, we just note it and tag following
            // initialization.
            super.visitTypeInsn( opcode, typeName );
            ++outstandingAllocs;
        }
        else if ( opcode == Opcodes.ANEWARRAY )
        {
            super.visitInsn( Opcodes.DUP );
            super.visitTypeInsn( opcode, typeName );
            invokeRecordAllocation( typeName );
        }
        else
        {
            super.visitTypeInsn( opcode, typeName );
        }
    }

    /**
     * Called by the ASM framework once the class is done being visited to
     * compute stack & local variable count maximums.
     */
    @Override
    public void visitMaxs( int maxStack, int maxLocals )
    {
        if ( localScopes != null )
        {
            for ( VariableScope scope : localScopes )
            {
                super.visitLocalVariable( "xxxxx$" + scope.index, scope.desc, null,
                        scope.start, scope.end, scope.index );
            }
        }
        super.visitMaxs( maxStack, maxLocals );
    }

    // Helper method to allocate a new local variable and account for its scope.
    private int newLocal( Type type, String typeDesc,
                          Label begin, Label end )
    {
        int newVar = lvs.newLocal( type );
        getLocalScopes().add( new VariableScope( newVar, begin, end, typeDesc ) );
        return newVar;
    }

    // Sometimes I happen to have a string descriptor and sometimes a type;
    // these alternate versions let me avoid recomputing whatever I already
    // know.
    private int newLocal( String typeDescriptor, Label begin, Label end )
    {
        return newLocal( Type.getType( typeDescriptor ), typeDescriptor, begin, end );
    }

    private int newLocal( Type type, Label begin, Label end )
    {
        return newLocal( type, type.getDescriptor(), begin, end );
    }

    // Helper method to actually invoke the recorder function for an allocation
    // event.
    // pre: stack: ... count newobj
    // post: stack: ... newobj
    private void invokeRecordAllocation( String typeName )
    {
        typeName = typeName.replaceAll( "^\\[*L", "" ).replaceAll( ";$", "" );
        // stack: ... count newobj
        super.visitInsn( Opcodes.DUP_X1 );
        // -> stack: ... newobj count newobj
        super.visitLdcInsn( typeName );
        // -> stack: ... newobj count newobj typename
        super.visitInsn( Opcodes.SWAP );
        // -> stack: ... newobj count typename newobj
        super.visitMethodInsn( Opcodes.INVOKESTATIC,
                recorderClass, recorderMethod, RECORDER_SIGNATURE );
        // -> stack: ... newobj
    }

    /**
     * multianewarray gets its very own visit method in the ASM framework, so we
     * hook it here.  This bytecode is different from most in that it consumes a
     * variable number of stack elements during execution.  The number of stack
     * elements consumed is specified by the dimCount operand.
     */
    @Override
    public void visitMultiANewArrayInsn( String typeName, int dimCount )
    {
        // stack: ... dim1 dim2 dim3 ... dimN
        super.visitMultiANewArrayInsn( typeName, dimCount );
        // -> stack: ... aref
        calculateArrayLengthAndDispatch( typeName, dimCount );
    }

    void calculateArrayLengthAndDispatch( String typeName, int dimCount )
    {
        // Since the dimensions of the array are not known at instrumentation
        // time, we take the created multi-dimensional array and peel off nesting
        // levels from the left.  For each nesting layer we probe the array length
        // and accumulate a partial product which we can then feed the recording
        // function.

        // below we note the partial product of dimensions 1 to X-1 as productToX
        // (so productTo1 == 1 == no dimensions yet).  We denote by aref0 the
        // array reference at the current nesting level (the containing aref's [0]
        // element).  If we hit a level whose arraylength is 0 there's no point
        // continuing so we shortcut out.
        Label zeroDimension = new Label();
        super.visitInsn( Opcodes.DUP ); // -> stack: ... origaref aref0
        super.visitLdcInsn( 1 ); // -> stack: ... origaref aref0 productTo1
        for ( int i = 0; i < dimCount; ++i )
        {
            // pre: stack: ... origaref aref0 productToI
            super.visitInsn( Opcodes.SWAP ); // -> stack: ... origaref productToI aref
            super.visitInsn( Opcodes.DUP_X1 );
            // -> stack: ... origaref aref0 productToI aref
            super.visitInsn( Opcodes.ARRAYLENGTH );
            // -> stack: ... origaref aref0 productToI dimI

            Label nonZeroDimension = new Label();
            super.visitInsn( Opcodes.DUP );
            // -> stack: ... origaref aref0 productToI dimI dimI
            super.visitJumpInsn( Opcodes.IFNE, nonZeroDimension );
            // -> stack: ... origaref aref0 productToI dimI
            super.visitInsn( Opcodes.POP );
            // -> stack: ... origaref aref0 productToI
            super.visitJumpInsn( Opcodes.GOTO, zeroDimension );
            super.visitLabel( nonZeroDimension );
            // -> stack: ... origaref aref0 productToI max(dimI,1)

            super.visitInsn( Opcodes.IMUL );
            // -> stack: ... origaref aref0 productTo{I+1}
            if ( i < dimCount - 1 )
            {
                super.visitInsn( Opcodes.SWAP );
                // -> stack: ... origaref productTo{I+1} aref0
                super.visitInsn( Opcodes.ICONST_0 );
                // -> stack: ... origaref productTo{I+1} aref0 0
                super.visitInsn( Opcodes.AALOAD );
                // -> stack: ... origaref productTo{I+1} aref0'
                super.visitInsn( Opcodes.SWAP );
            }
            // post: stack: ... origaref aref0 productTo{I+1}
        }
        super.visitLabel( zeroDimension );

        super.visitInsn( Opcodes.SWAP ); // -> stack: ... origaref product aref0
        super.visitInsn( Opcodes.POP ); // -> stack: ... origaref product
        super.visitInsn( Opcodes.SWAP ); // -> stack: ... product origaref
        invokeRecordAllocation( typeName );
    }
}
