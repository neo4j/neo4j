/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.codegen;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

interface ByteCodeVisitor
{
    interface Configurable
    {
        void addByteCodeVisitor( ByteCodeVisitor visitor );
    }

    ByteCodeVisitor DO_NOTHING = new ByteCodeVisitor()
    {
        @Override
        public void visitByteCode( String name, ByteBuffer bytes )
        {
        }
    };

    void visitByteCode( String name, ByteBuffer bytes );

    class Multiplex implements ByteCodeVisitor
    {
        private final ByteCodeVisitor[] visitors;

        public Multiplex( ByteCodeVisitor[] visitors )
        {
            this.visitors = visitors;
        }

        @Override
        public void visitByteCode( String name, ByteBuffer bytes )
        {
            for ( ByteCodeVisitor visitor : visitors )
            {
                visitor.visitByteCode( name, bytes.duplicate() );
            }
        }
    }

    class Printer extends ClassVisitor implements ByteCodeVisitor
    {
        public static final int API = Opcodes.ASM4;
        private final PrintStream out;

        Printer( PrintStream out )
        {
            super( API );
            this.out = out;
        }

        @Override
        public void visitByteCode( String name, ByteBuffer bytes )
        {
            new ClassReader( bytes.array() ).accept( this, 0 );
        }

        @Override
        public void visit( int version, int access, String name, String signature, String superName,
                           String[] interfaces )
        {
            StringBuilder iFaces = new StringBuilder();
            String prefix = " implements ";
            for ( String iFace : interfaces )
            {
                iFaces.append( prefix ).append( iFace );
                prefix = ", ";
            }
            out.format( "%s class %s extends %s%s%n{%n", Modifier.toString( access ), name, superName, iFaces );
        }

        @Override
        public FieldVisitor visitField( int access, String name, String desc, String signature, Object value )
        {
            out.format( "%s %s %s%s;", Modifier.toString( access ), desc, name, value == null ? "" : (" = " + value) );
            return super.visitField( access, name, desc, signature, value );
        }

        @Override
        public MethodVisitor visitMethod( int access, String name, String desc, String signature, String[] exceptions )
        {
            out.format( "  %s %s%s%n  {%n", Modifier.toString( access ), name, desc );
            return new MethodVisitor( API )
            {
                @Override
                public void visitFrame( int type, int nLocal, Object[] local, int nStack, Object[] stack )
                {
                    StringBuilder frame = new StringBuilder().append( "    [FRAME:" );
                    switch ( type )
                    {
                    case Opcodes.F_NEW:
                        frame.append( "NEW" );
                        break;
                    case Opcodes.F_FULL:
                        frame.append( "FULL" );
                        break;
                    case Opcodes.F_APPEND:
                        frame.append( "APPEND" );
                        break;
                    case Opcodes.F_CHOP:
                        frame.append( "CHOP" );
                        break;
                    case Opcodes.F_SAME:
                        frame.append( "SAME" );
                        break;
                    case Opcodes.F_SAME1:
                        frame.append( "SAME1" );
                        break;
                    default:
                        frame.append( type );
                    }
                    frame.append( ", " ).append( nLocal ).append( " locals: [" );
                    String prefix = "";
                    for ( int i = 0; i < nLocal; i++ )
                    {
                        frame.append( prefix );
                        if ( local[i] instanceof String )
                        {
                            frame.append( local[i] );
                        }
                        else if ( local[i] == Opcodes.TOP )
                        {
                            frame.append( "TOP" );
                        }
                        else if ( local[i] == Opcodes.INTEGER )
                        {
                            frame.append( "INTEGER" );
                        }
                        else if ( local[i] == Opcodes.FLOAT )
                        {
                            frame.append( "FLOAT" );
                        }
                        else if ( local[i] == Opcodes.DOUBLE )
                        {
                            frame.append( "DOUBLE" );
                        }
                        else if ( local[i] == Opcodes.LONG )
                        {
                            frame.append( "LONG" );
                        }
                        else if ( local[i] == Opcodes.NULL )
                        {
                            frame.append( "NULL" );
                        }
                        else if ( local[i] == Opcodes.UNINITIALIZED_THIS )
                        {
                            frame.append( "UNINITIALIZED_THIS" );
                        }
                        else
                        {
                            frame.append( local[i] );
                        }
                        prefix = ", ";
                    }
                    frame.append( "], " ).append( nStack ).append( " items on stack: [" );
                    prefix = "";
                    for ( int i = 0; i < nStack; i++ )
                    {
                        frame.append( prefix ).append( Objects.toString( stack[i] ) );
                        prefix = ", ";
                    }
                    out.println( frame.append( "]" ) );
                }

                @Override
                public void visitInsn( int opcode )
                {
                    out.format( "    %s%n", opcode( opcode ) );
                }

                @Override
                public void visitIntInsn( int opcode, int operand )
                {
                    out.format( "    %s %d%n", opcode( opcode ), operand );
                }

                @Override
                public void visitVarInsn( int opcode, int var )
                {
                    out.format( "    %s var:%d%n", opcode( opcode ), var );
                }

                @Override
                public void visitTypeInsn( int opcode, String type )
                {
                    out.format( "    %s %s%n", opcode( opcode ), type );
                }

                @Override
                public void visitFieldInsn( int opcode, String owner, String name, String desc )
                {
                    out.format( "    %s %s.%s:%s%n", opcode( opcode ), owner, name, desc );
                }

                @Override
                public void visitMethodInsn( int opcode, String owner, String name, String desc, boolean itf )
                {
                    out.format( "    %s %s.%s%s%n", opcode( opcode ), owner, name, desc );
                }

                @Override
                public void visitInvokeDynamicInsn( String name, String desc, Handle bsm, Object... bsmArgs )
                {
                    out.format( "    InvokeDynamic %s%s / bsm:%s%s%n", name, desc, bsm, Arrays.toString( bsmArgs ) );
                }

                @Override
                public void visitJumpInsn( int opcode, Label label )
                {
                    out.format( "    %s %s%n", opcode( opcode ), label );
                }

                @Override
                public void visitLabel( Label label )
                {
                    out.format( "   %s:%n", label );
                }

                @Override
                public void visitLdcInsn( Object cst )
                {
                    out.format( "    LDC %s%n", cst );
                }

                @Override
                public void visitIincInsn( int var, int increment )
                {
                    out.format( "    IINC %d += %d%n", var, increment );
                }

                @Override
                public void visitTableSwitchInsn( int min, int max, Label dflt, Label... labels )
                {
                    out.format( "    TABLE_SWITCH(min=%d, max=%d)%n    {%n", min, max );
                    for ( int i = 0, val = min; i < labels.length; i++, val++ )
                    {
                        out.format( "      case %d goto %s%n", val, labels[i] );
                    }
                    out.printf( "      default goto %s%n    }%n", dflt );
                }

                @Override
                public void visitLookupSwitchInsn( Label dflt, int[] keys, Label[] labels )
                {
                    out.format( "    LOOKUP_SWITCH%n    {%n" );
                    for ( int i = 0; i < labels.length; i++ )
                    {
                        out.format( "      case %d goto %s%n", keys[i], labels[i] );
                    }
                    out.printf( "      default goto %s%n    }%n", dflt );
                }

                @Override
                public void visitMultiANewArrayInsn( String desc, int dims )
                {
                    out.format( "    MULTI_ANEW_ARRAY %s, dims:%d%n", desc, dims );
                }

                @Override
                public void visitTryCatchBlock( Label start, Label end, Label handler, String type )
                {
                    out.format( "    [try/catch %s start@%s, end@%s, handler@%s]%n", type, start, end, handler );
                }

                @Override
                public void visitLocalVariable( String name, String desc, String signature, Label start, Label end,
                                                int index )
                {
                    out.format( "    [local %s:%s, from %s to %s @offset=%d]%n", name, desc, start, end, index );
                }

                @Override
                public void visitLineNumber( int line, Label start )
                {
                    out.format( "    [line %d @ %s]%n", line, start );
                }

                @Override
                public void visitEnd()
                {
                    out.println( "  }" );
                }
            };
        }

        private static String opcode( int opcode )
        {
            switch ( opcode )
            {
            // visitInsn
            case Opcodes.NOP:
                return "NOP";
            case Opcodes.ACONST_NULL:
                return "ACONST_NULL";
            case Opcodes.ICONST_M1:
                return "ICONST_M1";
            case Opcodes.ICONST_0:
                return "ICONST_0";
            case Opcodes.ICONST_1:
                return "ICONST_1";
            case Opcodes.ICONST_2:
                return "ICONST_2";
            case Opcodes.ICONST_3:
                return "ICONST_3";
            case Opcodes.ICONST_4:
                return "ICONST_4";
            case Opcodes.ICONST_5:
                return "ICONST_5";
            case Opcodes.LCONST_0:
                return "LCONST_0";
            case Opcodes.LCONST_1:
                return "LCONST_1";
            case Opcodes.FCONST_0:
                return "FCONST_0";
            case Opcodes.FCONST_1:
                return "FCONST_1";
            case Opcodes.FCONST_2:
                return "FCONST_2";
            case Opcodes.DCONST_0:
                return "DCONST_0";
            case Opcodes.DCONST_1:
                return "DCONST_1";
            case Opcodes.IALOAD:
                return "IALOAD";
            case Opcodes.LALOAD:
                return "LALOAD";
            case Opcodes.FALOAD:
                return "FALOAD";
            case Opcodes.DALOAD:
                return "DALOAD";
            case Opcodes.AALOAD:
                return "AALOAD";
            case Opcodes.BALOAD:
                return "BALOAD";
            case Opcodes.CALOAD:
                return "CALOAD";
            case Opcodes.SALOAD:
                return "SALOAD";
            case Opcodes.IASTORE:
                return "IASTORE";
            case Opcodes.LASTORE:
                return "LASTORE";
            case Opcodes.FASTORE:
                return "FASTORE";
            case Opcodes.DASTORE:
                return "DASTORE";
            case Opcodes.AASTORE:
                return "AASTORE";
            case Opcodes.BASTORE:
                return "BASTORE";
            case Opcodes.CASTORE:
                return "CASTORE";
            case Opcodes.SASTORE:
                return "SASTORE";
            case Opcodes.POP:
                return "POP";
            case Opcodes.POP2:
                return "POP2";
            case Opcodes.DUP:
                return "DUP";
            case Opcodes.DUP_X1:
                return "DUP_X1";
            case Opcodes.DUP_X2:
                return "DUP_X2";
            case Opcodes.DUP2:
                return "DUP2";
            case Opcodes.DUP2_X1:
                return "DUP2_X1";
            case Opcodes.DUP2_X2:
                return "DUP2_X2";
            case Opcodes.SWAP:
                return "SWAP";
            case Opcodes.IADD:
                return "IADD";
            case Opcodes.LADD:
                return "LADD";
            case Opcodes.FADD:
                return "FADD";
            case Opcodes.DADD:
                return "DADD";
            case Opcodes.ISUB:
                return "ISUB";
            case Opcodes.LSUB:
                return "LSUB";
            case Opcodes.FSUB:
                return "FSUB";
            case Opcodes.DSUB:
                return "DSUB";
            case Opcodes.IMUL:
                return "IMUL";
            case Opcodes.LMUL:
                return "LMUL";
            case Opcodes.FMUL:
                return "FMUL";
            case Opcodes.DMUL:
                return "DMUL";
            case Opcodes.IDIV:
                return "IDIV";
            case Opcodes.LDIV:
                return "LDIV";
            case Opcodes.FDIV:
                return "FDIV";
            case Opcodes.DDIV:
                return "DDIV";
            case Opcodes.IREM:
                return "IREM";
            case Opcodes.LREM:
                return "LREM";
            case Opcodes.FREM:
                return "FREM";
            case Opcodes.DREM:
                return "DREM";
            case Opcodes.INEG:
                return "INEG";
            case Opcodes.LNEG:
                return "LNEG";
            case Opcodes.FNEG:
                return "FNEG";
            case Opcodes.DNEG:
                return "DNEG";
            case Opcodes.ISHL:
                return "ISHL";
            case Opcodes.LSHL:
                return "LSHL";
            case Opcodes.ISHR:
                return "ISHR";
            case Opcodes.LSHR:
                return "LSHR";
            case Opcodes.IUSHR:
                return "IUSHR";
            case Opcodes.LUSHR:
                return "LUSHR";
            case Opcodes.IAND:
                return "IAND";
            case Opcodes.LAND:
                return "LAND";
            case Opcodes.IOR:
                return "IOR";
            case Opcodes.LOR:
                return "LOR";
            case Opcodes.IXOR:
                return "IXOR";
            case Opcodes.LXOR:
                return "LXOR";
            case Opcodes.I2L:
                return "I2L";
            case Opcodes.I2F:
                return "I2F";
            case Opcodes.I2D:
                return "I2D";
            case Opcodes.L2I:
                return "L2I";
            case Opcodes.L2F:
                return "L2F";
            case Opcodes.L2D:
                return "L2D";
            case Opcodes.F2I:
                return "F2I";
            case Opcodes.F2L:
                return "F2L";
            case Opcodes.F2D:
                return "F2D";
            case Opcodes.D2I:
                return "D2I";
            case Opcodes.D2L:
                return "D2L";
            case Opcodes.D2F:
                return "D2F";
            case Opcodes.I2B:
                return "I2B";
            case Opcodes.I2C:
                return "I2C";
            case Opcodes.I2S:
                return "I2S";
            case Opcodes.LCMP:
                return "LCMP";
            case Opcodes.FCMPL:
                return "FCMPL";
            case Opcodes.FCMPG:
                return "FCMPG";
            case Opcodes.DCMPL:
                return "DCMPL";
            case Opcodes.DCMPG:
                return "DCMPG";
            case Opcodes.IRETURN:
                return "IRETURN";
            case Opcodes.LRETURN:
                return "LRETURN";
            case Opcodes.FRETURN:
                return "FRETURN";
            case Opcodes.DRETURN:
                return "DRETURN";
            case Opcodes.ARETURN:
                return "ARETURN";
            case Opcodes.RETURN:
                return "RETURN";
            case Opcodes.ARRAYLENGTH:
                return "ARRAYLENGTH";
            case Opcodes.ATHROW:
                return "ATHROW";
            case Opcodes.MONITORENTER:
                return "MONITORENTER";
            case Opcodes.MONITOREXIT:
                return "MONITOREXIT";
            // visitIntInsn
            case Opcodes.BIPUSH:
                return "BIPUSH";
            case Opcodes.SIPUSH:
                return "SIPUSH";
            case Opcodes.NEWARRAY:
                return "NEWARRAY";
            // visitVarInsn
            case Opcodes.ILOAD:
                return "ILOAD";
            case Opcodes.LLOAD:
                return "LLOAD";
            case Opcodes.FLOAD:
                return "FLOAD";
            case Opcodes.DLOAD:
                return "DLOAD";
            case Opcodes.ALOAD:
                return "ALOAD";
            case Opcodes.ISTORE:
                return "ISTORE";
            case Opcodes.LSTORE:
                return "LSTORE";
            case Opcodes.FSTORE:
                return "FSTORE";
            case Opcodes.DSTORE:
                return "DSTORE";
            case Opcodes.ASTORE:
                return "ASTORE";
            case Opcodes.RET:
                return "RET";
            // visitTypeInsn
            case Opcodes.NEW:
                return "NEW";
            case Opcodes.ANEWARRAY:
                return "ANEWARRAY";
            case Opcodes.CHECKCAST:
                return "CHECKCAST";
            case Opcodes.INSTANCEOF:
                return "INSTANCEOF";
            // visitFieldInsn
            case Opcodes.GETSTATIC:
                return "GETSTATIC";
            case Opcodes.PUTSTATIC:
                return "PUTSTATIC";
            case Opcodes.GETFIELD:
                return "GETFIELD";
            case Opcodes.PUTFIELD:
                return "PUTFIELD";
            // visitMethodInsn
            case Opcodes.INVOKEVIRTUAL:
                return "INVOKEVIRTUAL";
            case Opcodes.INVOKESPECIAL:
                return "INVOKESPECIAL";
            case Opcodes.INVOKESTATIC:
                return "INVOKESTATIC";
            case Opcodes.INVOKEINTERFACE:
                return "INVOKEINTERFACE";
            // visitJumpInsn
            case Opcodes.IFEQ:
                return "IFEQ";
            case Opcodes.IFNE:
                return "IFNE";
            case Opcodes.IFLT:
                return "IFLT";
            case Opcodes.IFGE:
                return "IFGE";
            case Opcodes.IFGT:
                return "IFGT";
            case Opcodes.IFLE:
                return "IFLE";
            case Opcodes.IF_ICMPEQ:
                return "IF_ICMPEQ";
            case Opcodes.IF_ICMPNE:
                return "IF_ICMPNE";
            case Opcodes.IF_ICMPLT:
                return "IF_ICMPLT";
            case Opcodes.IF_ICMPGE:
                return "IF_ICMPGE";
            case Opcodes.IF_ICMPGT:
                return "IF_ICMPGT";
            case Opcodes.IF_ICMPLE:
                return "IF_ICMPLE";
            case Opcodes.IF_ACMPEQ:
                return "IF_ACMPEQ";
            case Opcodes.IF_ACMPNE:
                return "IF_ACMPNE";
            case Opcodes.GOTO:
                return "GOTO";
            case Opcodes.JSR:
                return "JSR";
            case Opcodes.IFNULL:
                return "IFNULL";
            case Opcodes.IFNONNULL:
                return "IFNONNULL";
            default:
                throw new IllegalArgumentException( "unknown opcode: " + opcode );
            }
        }

        @Override
        public void visitEnd()
        {
            out.println( "}" );
        }
    }
}
