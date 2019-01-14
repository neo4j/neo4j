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

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.SimpleVerifier;
import org.objectweb.asm.tree.analysis.Value;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.IntFunction;
import javax.tools.Diagnostic;

import org.neo4j.codegen.ByteCodes;
import org.neo4j.codegen.CodeGeneratorOption;
import org.neo4j.codegen.CompilationFailureException;

import static org.objectweb.asm.ClassReader.SKIP_DEBUG;

@SuppressWarnings( "unused"/*used through reflection, since it depends on optional code*/ )
class ByteCodeVerifier implements ByteCodeChecker, CodeGeneratorOption
{
    /**
     * Invoked by {@link ByteCode#load(String)} to load this class.
     *
     * @return an instance of this class, if all dependencies are available.
     */
    static CodeGeneratorOption loadVerifier()
    {
        if ( Analyzer.class.getName().isEmpty() || CheckClassAdapter.class.getName().isEmpty() )
        {
            throw new AssertionError( "This code is here to ensure the optional ASM classes are on the classpath" );
        }
        return new ByteCodeVerifier();
    }

    /**
     * Add this verification step to the configuration, if applicable.
     *
     * @param target
     *         the configuration to add this verification step to.
     */
    @Override
    public void applyTo( Object target )
    {
        if ( target instanceof Configuration )
        {
            ((Configuration) target).withBytecodeChecker( this );
        }
    }

    /**
     * Check the bytecode from one round of bytecode generation.
     *
     * @param classpathLoader
     *         the ClassLoader to use for loading classes from the classpath.
     * @param byteCodes
     *         the bytecodes generated in this round.
     * @throws CompilationFailureException
     *         if any issue is discovered in the verification.
     */
    @Override
    public void check( ClassLoader classpathLoader, Collection<ByteCodes> byteCodes ) throws CompilationFailureException
    {
        List<ClassNode> classes = new ArrayList<>( byteCodes.size() );
        List<Failure> failures = new ArrayList<>();
        // load (and verify) the structure of the generated classes
        for ( ByteCodes byteCode : byteCodes )
        {
            try
            {
                classes.add( classNode( byteCode.bytes() ) );
            }
            catch ( Exception e )
            {
                failures.add( new Failure( e, e.toString() ) );
            }
        }
        // if there are problems with the structure of the generated classes,
        // we are not going to be able to verify their methods
        if ( !failures.isEmpty() )
        {
            throw compilationFailure( failures );
        }
        // continue with verifying the methods of the classes
        AssignmentChecker check = new AssignmentChecker( classpathLoader, classes );
        for ( ClassNode clazz : classes )
        {
            verify( check, clazz, failures );
        }
        if ( !failures.isEmpty() )
        {
            throw compilationFailure( failures );
        }
    }

    /**
     * Verify the methods of a single class.
     *
     * @param check
     *         a helper for verifying assignments.
     * @param clazz
     *         the class to check the methods of.
     * @param failures
     *         where any detected errors are added.
     */
    private static void verify( AssignmentChecker check, ClassNode clazz, List<Failure> failures )
    {
        Verifier verifier = new Verifier( clazz, check );
        for ( MethodNode method : clazz.methods )
        {
            Analyzer<?> analyzer = new Analyzer<>( verifier );
            try
            {
                analyzer.analyze( clazz.name, method );
            }
            catch ( Exception cause )
            {
                failures.add( new Failure( cause, detailedMessage(
                        cause.getMessage(),
                        method,
                        analyzer.getFrames(),
                        cause instanceof AnalyzerException ? ((AnalyzerException) cause).node : null ) ) );
            }
        }
    }

    private static ClassNode classNode( ByteBuffer bytecode )
    {
        byte[] bytes;
        if ( bytecode.hasArray() )
        {
            bytes = bytecode.array();
        }
        else
        {
            bytes = new byte[bytecode.limit()];
            bytecode.get( bytes );
        }
        ClassNode classNode = new ClassNode();
        new ClassReader( bytes ).accept( new CheckClassAdapter( classNode, false ), SKIP_DEBUG );
        return classNode;
    }

    private static CompilationFailureException compilationFailure( List<Failure> failures )
    {
        List<Diagnostic<?>> diagnostics = new ArrayList<>( failures.size() );
        for ( Failure failure : failures )
        {
            diagnostics.add( new BytecodeDiagnostic( failure.message ) );
        }
        CompilationFailureException exception = new CompilationFailureException( diagnostics );
        for ( Failure failure : failures )
        {
            exception.addSuppressed( failure.cause );
        }
        return exception;
    }

    private static class Failure
    {
        final Throwable cause;
        final String message;

        Failure( Throwable cause, String message )
        {
            this.cause = cause;
            this.message = message;
        }
    }

    private static String detailedMessage(
            String errorMessage,
            MethodNode method,
            Frame[] frames,
            AbstractInsnNode errorLocation )
    {
        StringWriter message = new StringWriter();
        try ( PrintWriter out = new PrintWriter( message ) )
        {
            List<Integer> localLengths = new ArrayList<>();
            List<Integer> stackLengths = new ArrayList<>();
            for ( Frame frame : frames )
            {
                if ( frame != null )
                {
                    for ( int i = 0; i < frame.getLocals(); i++ )
                    {
                        insert( i, frame.getLocal( i ), localLengths );
                    }
                    for ( int i = 0; i < frame.getStackSize(); i++ )
                    {
                        insert( i, frame.getStack( i ), stackLengths );
                    }
                }
            }
            Textifier formatted = new Textifier();
            TraceMethodVisitor mv = new TraceMethodVisitor( formatted );

            out.println( errorMessage );
            out.append( "\t\tin " ).append( method.name ).append( method.desc ).println();
            for ( int i = 0; i < method.instructions.size(); i++ )
            {
                AbstractInsnNode insn = method.instructions.get( i );
                insn.accept( mv );
                Frame frame = frames[i];
                out.append( "\t\t" );
                out.append( insn == errorLocation ? ">>> " : "    " );
                out.format( "%05d [", i );
                if ( frame == null )
                {
                    padding( out, localLengths.listIterator(), '?' );
                    out.append( " : " );
                    padding( out, stackLengths.listIterator(), '?' );
                }
                else
                {
                    emit( out, localLengths, frame::getLocal, frame.getLocals() );
                    padding( out, localLengths.listIterator( frame.getLocals() ), '-' );
                    out.append( " : " );
                    emit( out, stackLengths, frame::getStack, frame.getStackSize() );
                    padding( out, stackLengths.listIterator( frame.getStackSize() ), ' ' );
                }
                out.print( "] : " );
                out.print( formatted.text.get( formatted.text.size() - 1 ) );
            }
            for ( int j = 0; j < method.tryCatchBlocks.size(); j++ )
            {
                method.tryCatchBlocks.get( j ).accept( mv );
                out.print( " " + formatted.text.get( formatted.text.size() - 1 ) );
            }
        }
        return message.toString();
    }

    private static void emit( PrintWriter out, List<Integer> lengths, IntFunction<Value> valueLookup, int values )
    {
        for ( int i = 0; i < values; i++ )
        {
            if ( i > 0 )
            {
                out.append( ' ' );
            }
            String name = shortName( valueLookup.apply( i ).toString() );
            for ( int pad = lengths.get( i ) - name.length(); pad-- > 0; )
            {
                out.append( ' ' );
            }
            out.append( name );
        }
    }

    private static void padding( PrintWriter out, ListIterator<Integer> lengths, char var )
    {
        while ( lengths.hasNext() )
        {
            if ( lengths.nextIndex() > 0 )
            {
                out.append( ' ' );
            }
            for ( int length = lengths.next(); length-- > 1; )
            {
                out.append( ' ' );
            }
            out.append( var );
        }
    }

    private static void insert( int i, Value value, List<Integer> values )
    {
        int length = shortName( value.toString() ).length();
        while ( i >= values.size() )
        {
            values.add( 1 );
        }
        if ( length > values.get( i ) )
        {
            values.set( i, length );
        }
    }

    private static String shortName( String name )
    {
        int start = name.lastIndexOf( '/' );
        int end = name.length();
        if ( name.charAt( end - 1 ) == ';' )
        {
            end--;
        }
        return start == -1 ? name : name.substring( start + 1, end );
    }

    // This class might look failed in the IDE, but javac will accept it
    // The reason is because the base class has been re-written to work on old JVMs - generics have been dropped.
    private static class Verifier extends SimpleVerifier
    {
        private final AssignmentChecker check;

        Verifier( ClassNode clazz, AssignmentChecker check )
        {
            super( Opcodes.ASM6, Type.getObjectType( clazz.name ), superClass( clazz ), interfaces( clazz ),
                    isInterfaceNode( clazz ) );
            this.check = check;
        }

        @Override
        protected boolean isAssignableFrom( Type target, Type value )
        {
            return check.isAssignableFrom( target, value );
        }

        @Override
        protected boolean isSubTypeOf( BasicValue value, BasicValue expected )
        {
            return super.isSubTypeOf( value, expected ) || check
                    .invokableInterface( expected.getType(), value.getType() );
        }

        private static Type superClass( ClassNode clazz )
        {
            return clazz.superName == null ? null : Type.getObjectType( clazz.superName );
        }

        @SuppressWarnings( "unchecked" )
        private static List<Type> interfaces( ClassNode clazz )
        {
            List<Type> interfaces = new ArrayList<>( clazz.interfaces.size() );
            for ( String iFace : clazz.interfaces )
            {
                interfaces.add( Type.getObjectType( iFace ) );
            }
            return interfaces;
        }
    }

    private static class AssignmentChecker
    {
        private final ClassLoader classpathLoader;
        private final Map<Type,ClassNode> classes = new HashMap<>();

        AssignmentChecker( ClassLoader classpathLoader, List<ClassNode> classes )
        {
            this.classpathLoader = classpathLoader;
            for ( ClassNode node : classes )
            {
                this.classes.put( Type.getObjectType( node.name ), node );
            }
        }

        boolean invokableInterface( Type target, Type value )
        {
            // this method allows a bit too much through,
            // it really ought to only be used for the target type of INVOKEINTERFACE,
            // since any object type is allowed as target for INVOKEINTERFACE,
            // this allows fewer CHECKCAST instructions when using generics.
            ClassNode targetNode = classes.get( target );
            if ( targetNode != null )
            {
                if ( isInterfaceNode( targetNode ) )
                {
                    return value.getSort() == Type.OBJECT || value.getSort() == Type.ARRAY;
                }
                return false;
            }
            Class<?> targetClass = getClass( target );
            if ( targetClass.isInterface() )
            {
                return value.getSort() == Type.OBJECT || value.getSort() == Type.ARRAY;
            }
            return false;
        }

        boolean isAssignableFrom( Type target, Type value )
        {
            if ( target.equals( value ) )
            {
                return true;
            }
            ClassNode targetNode = classes.get( target );
            ClassNode valueNode = classes.get( value );
            if ( targetNode != null && valueNode == null )
            {
                // if the target is among the types we have generated and the value isn't, then
                // the value type either doesn't exist, or it is defined in the class loader, and thus cannot
                // be a subtype of the target type
                return false;
            }
            else if ( valueNode != null )
            {
                return isAssignableFrom( target, valueNode );
            }
            else
            {
                return getClass( target ).isAssignableFrom( getClass( value ) );
            }
        }

        private boolean isAssignableFrom( Type target, ClassNode value )
        {
            if ( value.superName != null && isAssignableFrom( target, Type.getObjectType( value.superName ) ) )
            {
                return true;
            }
            for ( String iFace : value.interfaces )
            {
                if ( isAssignableFrom( target, Type.getObjectType( iFace ) ) )
                {
                    return true;
                }
            }
            return false;
        }

        private Class<?> getClass( Type type )
        {
            try
            {
                if ( type.getSort() == Type.ARRAY )
                {
                    return Class.forName( type.getDescriptor().replace( '/', '.' ), false, classpathLoader );
                }
                return Class.forName( type.getClassName(), false, classpathLoader );
            }
            catch ( ClassNotFoundException e )
            {
                throw new RuntimeException( e.toString() );
            }
        }
    }

    private static boolean isInterfaceNode( ClassNode clazz )
    {
        return (clazz.access & Opcodes.ACC_INTERFACE) != 0;
    }
}

