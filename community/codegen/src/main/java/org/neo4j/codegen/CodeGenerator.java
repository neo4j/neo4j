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

import static java.util.Objects.requireNonNull;
import static org.neo4j.codegen.ByteCodeVisitor.DO_NOTHING;
import static org.neo4j.codegen.CodeGenerationStrategy.codeGenerator;
import static org.neo4j.codegen.TypeReference.OBJECT;
import static org.neo4j.codegen.TypeReference.typeReference;
import static org.neo4j.codegen.TypeReference.typeReferences;

public abstract class CodeGenerator
{
    private final CodeLoader loader;
    private long generation, classes;
    private ByteCodeVisitor byteCodeVisitor = DO_NOTHING;

    public static CodeGenerator generateCode( CodeGeneratorOption... options )
            throws CodeGenerationNotSupportedException
    {
        return generateCode( Thread.currentThread().getContextClassLoader(), options );
    }

    public static CodeGenerator generateCode( ClassLoader loader, CodeGeneratorOption... options )
            throws CodeGenerationNotSupportedException
    {
        return codeGenerator( requireNonNull( loader, "ClassLoader" ), options );
    }

    public CodeGenerator( ClassLoader loader )
    {
        this.loader = new CodeLoader( loader );
    }

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
        return generateClass( makeHandle( packageName, name, base ), base, interfaces );
    }

    private synchronized ClassHandle makeHandle( String packageName, String name, TypeReference parent )
    {
        classes++;
        return new ClassHandle( packageName, name, parent, this, generation );
    }

    private ClassGenerator generateClass( ClassHandle handle, TypeReference base, TypeReference... interfaces )
    {
        return new ClassGenerator( handle, generate( handle, base, interfaces ) );
    }

    protected abstract ClassEmitter generate( TypeReference type, TypeReference base, TypeReference... interfaces );

    protected abstract Iterable<? extends ByteCodes> compile( ClassLoader classpathLoader )
            throws CompilationFailureException;

    synchronized Class<?> loadClass( String name, long generation ) throws CompilationFailureException
    {
        if ( generation == this.generation )
        {
            if ( classes != 0 )
            {
                throw new IllegalStateException( "Compilation has not completed." );
            }
            this.generation++;
            loader.addSources( compile( loader.getParent() ), byteCodeVisitor );
        }
        try
        {
            return loader.loadClass( name );
        }
        catch ( ClassNotFoundException e )
        {
            throw new IllegalStateException( "Could not find defined class.", e );
        }
    }

    synchronized void closeClass()
    {
        classes--;
    }

    void setByteCodeVisitor( ByteCodeVisitor visitor )
    {
        this.byteCodeVisitor = visitor;
    }
}
