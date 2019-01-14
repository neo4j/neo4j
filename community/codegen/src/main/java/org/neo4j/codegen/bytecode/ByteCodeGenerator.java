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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.codegen.ByteCodes;
import org.neo4j.codegen.ClassEmitter;
import org.neo4j.codegen.CodeGenerator;
import org.neo4j.codegen.CompilationFailureException;
import org.neo4j.codegen.TypeReference;

class ByteCodeGenerator extends CodeGenerator
{
    private final Configuration configuration;
    private final Map<TypeReference,ClassByteCodeWriter> classes = new HashMap<>();

    ByteCodeGenerator( ClassLoader parentClassLoader, Configuration configuration )
    {
        super( parentClassLoader );
        this.configuration = configuration;
    }

    @Override
    protected ClassEmitter generate( TypeReference type, TypeReference base, TypeReference[] interfaces )
    {
        ClassByteCodeWriter codeWriter = new ClassByteCodeWriter( type, base, interfaces );
        synchronized ( this )
        {
            ClassByteCodeWriter old = classes.put( type, codeWriter );
            if ( old != null )
            {
                classes.put( type, old );
                throw new IllegalStateException( "Trying to generate class twice: " + type );
            }
        }

        return codeWriter;
    }

    @Override
    protected Iterable<? extends ByteCodes> compile( ClassLoader classpathLoader ) throws CompilationFailureException
    {
        List<ByteCodes> byteCodes = new ArrayList<>( classes.size() );
        for ( ClassByteCodeWriter writer : classes.values() )
        {
            byteCodes.add( writer.toByteCodes() );
        }
        ByteCodeChecker checker = configuration.bytecodeChecker();
        if ( checker != null )
        {
            checker.check( classpathLoader, byteCodes );
        }
        return byteCodes;
    }
}
