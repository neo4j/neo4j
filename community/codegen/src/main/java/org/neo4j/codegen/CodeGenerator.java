/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.codegen;

import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

import static java.util.Objects.requireNonNull;
import static org.neo4j.codegen.ByteCodeVisitor.DO_NOTHING;
import static org.neo4j.codegen.CodeGenerationStrategy.codeGenerator;
import static org.neo4j.codegen.TypeReference.OBJECT;
import static org.neo4j.codegen.TypeReference.typeReference;
import static org.neo4j.codegen.TypeReference.typeReferences;

/**
 * Do you also want to generate code? Start here.
 *
 * CodeGenerator is the entry point of code generation. I allows generation of multiple
 * dependent classes per compilation unit.
 * <p/>
 * Example use:
 *
 * <pre>
 *      ClassGenerator c1 = generator.generateClass(...)
 *
 *      // start generating class 1 using the c1 generator
 *
 *      ClassGenerator c2 = generator.generateClass(...)
 *
 *      // generate class 2 using c2
 *
 *      ClassHandle c2handle = c2.handle
 *      c2.close()
 *
 *      // use c2handle to generate newInstance calls in c1
 *
 *      ClassHandle c1handle = c1.handle
 *      c1.close()
 *
 *      c1handle.loadClass // Forces compilation on the whole unit (c1 + c2), which
 *                         // will stage them for loading in {@link CodeLoader}. c1 will
 *                         // then immediately be loaded and the class object returned.
 *                         // Sometime later when c2 is needed, it will also be loaded.
 *                         // This could be once a c1 instance is executed, and need to
 *                         // instantiate a c2, or if we reflectively access c1 for example.
 * </pre>
 */
public abstract class CodeGenerator
{
    private final CodeLoader loader;
    private long currentCompilationUnit;
    private long openClassCount;
    private ByteCodeVisitor byteCodeVisitor = DO_NOTHING;

    public static CodeGenerator generateCode( CodeGenerationStrategy<?> strategy, CodeGeneratorOption... options )
            throws CodeGenerationNotSupportedException
    {
        return generateCode( Thread.currentThread().getContextClassLoader(), strategy, options );
    }

    public static CodeGenerator generateCode( ClassLoader loader, CodeGenerationStrategy<?> strategy, CodeGeneratorOption... options )
            throws CodeGenerationNotSupportedException
    {
        return codeGenerator( requireNonNull( loader, "ClassLoader" ), strategy, options );
    }

    public CodeGenerator( ClassLoader loader )
    {
        this.loader = new CodeLoader( loader );
    }

    private synchronized ClassHandle openClass( String packageName, String name, TypeReference parent )
    {
        openClassCount++;
        return new ClassHandle( packageName, name, parent, this, currentCompilationUnit );
    }

    synchronized void closeClass()
    {
        openClassCount--;
    }

    @VisibleForTesting
    public void setByteCodeVisitor( ByteCodeVisitor visitor )
    {
        this.byteCodeVisitor = visitor;
    }

    // GENERATE

    public ClassGenerator generateClass( String packageName, String name, Class<?> firstInterface, Class<?>... more )
    {
        return generateClass( packageName, name, typeReferences( firstInterface, more ) );
    }

    public ClassGenerator generateClass( Class<?> base, String packageName, String name, Class<?>... interfaces )
    {
        return generateClass( typeReference( base ), packageName, name, typeReferences( interfaces ) );
    }

    public ClassGenerator generateClass( String packageName, String name, TypeReference... interfaces )
    {
        return generateClass( OBJECT, packageName, name, interfaces );
    }

    public ClassGenerator generateClass( TypeReference base, String packageName, String name,
            TypeReference... interfaces )
    {
        return generateClass( openClass( packageName, name, base ), base, interfaces );
    }

    private ClassGenerator generateClass( ClassHandle handle, TypeReference base, TypeReference... interfaces )
    {
        return new ClassGenerator( handle, generate( handle, base, interfaces ) );
    }

    protected abstract ClassWriter generate( TypeReference type, TypeReference base, TypeReference... interfaces );

    // COMPILE AND LOAD

    protected abstract Iterable<? extends ByteCodes> compile( ClassLoader classpathLoader )
            throws CompilationFailureException;

    synchronized Class<?> loadClass( String name, long generation ) throws CompilationFailureException
    {
        compileAndStageForLoading( generation );
        try
        {
            return loader.findClass( name );
        }
        catch ( ClassNotFoundException e )
        {
            throw new IllegalStateException( "Could not find defined class.", e );
        }
    }

    synchronized Class<?> loadAnonymousClass( String name, long generation ) throws CompilationFailureException
    {
        compileAndStageForLoading( generation );
        try
        {
            return loader.defineAnonymousClass( name );
        }
        catch ( ClassNotFoundException e )
        {
            throw new IllegalStateException( "Could not find defined class.", e );
        }
    }

    /**
     * Compile all classes in the target compilation unit, and stage them for loading.
     *
     * If the target compilation unit has already been compiled ({@code compilationUnit < this.compilationUnit}),
     * this method does nothing.
     *
     * @param compilationUnit the target compilation unit
     */
    private void compileAndStageForLoading( long compilationUnit ) throws CompilationFailureException
    {
        Preconditions.checkState( compilationUnit <= this.currentCompilationUnit, "Future compilation units are not supported" );

        if ( compilationUnit == this.currentCompilationUnit )
        {
            Preconditions.checkState( openClassCount == 0, "Compilation has not completed." );

            this.currentCompilationUnit++;
            loader.stageForLoading( compile( loader.getParent() ), byteCodeVisitor );
        }
    }
}
