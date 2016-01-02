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
import org.objectweb.asm.MethodVisitor;

import org.neo4j.codegen.ClassEmitter;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.Resource;
import org.neo4j.codegen.TypeReference;

import static org.neo4j.codegen.ByteCodeUtils.byteCodeName;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.V1_8;

class ClassByteCodeWriter implements ClassEmitter
{
    private final ClassWriter classWriter;

    ClassByteCodeWriter(TypeReference type, TypeReference base, TypeReference[] interfaces)
    {
        this.classWriter = new ClassWriter( ClassWriter.COMPUTE_MAXS );
        String[] iNames = new String[interfaces.length];
        for ( int i = 0; i < interfaces.length; i++ )
        {
            iNames[i] = interfaces[i].name();
        }
        classWriter.visit(V1_8, ACC_PUBLIC + ACC_SUPER, byteCodeName(type.packageName()), signature(type), base.name(), iNames);
    }

    @Override
    public MethodEmitter method( MethodDeclaration signature )
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public void field( FieldReference field, Expression value )
    {

    }

    @Override
    public void done()
    {

    }

    private static class MethodByteCodeEmitter implements MethodEmitter {

        private final MethodVisitor methodVisitor;

        public MethodByteCodeEmitter(ClassWriter classWriter, MethodDeclaration declaration)
        {
            this.methodVisitor = classWriter.visitMethod( ACC_PUBLIC, declaration.name(), desc( declaration ),
                    signature( declaration ), exceptions( declaration ) );
        }

        @Override
        public void done()
        {

        }

        @Override
        public void expression( Expression expression )
        {

        }

        @Override
        public void put( Expression target, FieldReference field, Expression value )
        {

        }

        @Override
        public void returns()
        {

        }

        @Override
        public void returns( Expression value )
        {

        }

        @Override
        public void assign( TypeReference type, String name, Expression value )
        {

        }

        @Override
        public void beginWhile( Expression test )
        {

        }

        @Override
        public void beginIf( Expression test )
        {

        }

        @Override
        public void beginFinally()
        {

        }

        @Override
        public void endBlock()
        {

        }

        @Override
        public void beginTry( Resource... resources )
        {

        }

        @Override
        public void throwException( Expression exception )
        {

        }

        @Override
        public void beginCatch( Parameter exception )
        {

        }

        @Override
        public void declare( LocalVariable local )
        {

        }

        @Override
        public void assign( LocalVariable local, Expression value )
        {

        }

        @Override
        public void beginForEach( Parameter local, Expression iterable )
        {

        }
    }

}
