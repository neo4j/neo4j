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

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.codegen.ByteCodes;
import org.neo4j.codegen.ClassEmitter;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.TypeReference;

import static org.neo4j.codegen.ByteCodeUtils.byteCodeName;
import static org.neo4j.codegen.ByteCodeUtils.outerName;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.typeName;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.PUTSTATIC;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_8;

class ClassByteCodeWriter implements ClassEmitter
{
    private final ClassWriter classWriter;
    private final ClassVisitor classVisitor;
    private final TypeReference type;
    private final Map<FieldReference,Expression> staticFields = new HashMap<>();
    private final TypeReference base;

    ClassByteCodeWriter( TypeReference type, TypeReference base, TypeReference[] interfaces )
    {
        this.classWriter = new ClassWriter( ClassWriter.COMPUTE_FRAMES );
        this.classVisitor = classWriter; // this separation is useful if we want to add intermediary visitors
        String[] iNames = new String[interfaces.length];
        for ( int i = 0; i < interfaces.length; i++ )
        {
            iNames[i] = byteCodeName( interfaces[i] );
        }
        classVisitor.visit( V1_8, ACC_PUBLIC + ACC_SUPER, byteCodeName( type ), signature( type ),
                byteCodeName( base ), iNames.length != 0 ? iNames : null );
        if ( base.isInnerClass() )
        {
            classVisitor.visitInnerClass( byteCodeName( base ), outerName( base ),
                    base.simpleName(), ACC_PUBLIC + ACC_STATIC );
        }
        this.type = type;
        this.base = base;
    }

    @Override
    public MethodEmitter method( MethodDeclaration signature )
    {
        return new MethodByteCodeEmitter( classVisitor, signature, base );
    }

    @Override
    public void field( FieldReference field, Expression value )
    {
        //keep track of all static field->value, and initiate in <clinit> in done
        if ( Modifier.isStatic( field.modifiers() ) && value != null )
        {
            staticFields.put( field, value );
        }
        FieldVisitor fieldVisitor = classVisitor
                .visitField( field.modifiers(), field.name(), typeName( field.type() ), signature( field.type() ),
                        null );
        fieldVisitor.visitEnd();
    }

    @Override
    public void done()
    {
        if ( !staticFields.isEmpty() )
        {
            MethodVisitor methodVisitor = classVisitor.visitMethod( ACC_STATIC, "<clinit>", "()V", null, null );
            ByteCodeExpressionVisitor expressionVisitor = new ByteCodeExpressionVisitor( methodVisitor );
            methodVisitor.visitCode();
            for ( Map.Entry<FieldReference,Expression> entry : staticFields.entrySet() )
            {
                FieldReference field = entry.getKey();
                Expression value = entry.getValue();
                value.accept( expressionVisitor );
                methodVisitor.visitFieldInsn( PUTSTATIC, byteCodeName( field.owner() ),
                        field.name(), typeName( field.type() ) );
            }
            methodVisitor.visitInsn( RETURN );
            methodVisitor.visitMaxs( 0, 0 );
            methodVisitor.visitEnd();
        }
        classVisitor.visitEnd();
    }

    ByteCodes toByteCodes()
    {
        byte[] bytecode = classWriter.toByteArray();
        return new ByteCodes()
        {
            @Override
            public String name()
            {
                return type.fullName();
            }

            @Override
            public ByteBuffer bytes()
            {
                return ByteBuffer.wrap( bytecode );
            }
        };
    }
}
