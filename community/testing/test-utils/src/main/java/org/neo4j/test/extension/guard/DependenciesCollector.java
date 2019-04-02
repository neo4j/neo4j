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
package org.neo4j.test.extension.guard;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.objectweb.asm.Type.getObjectType;
import static org.objectweb.asm.Type.getType;

public class DependenciesCollector extends ClassVisitor
{
    private static final int API_VERSION = Opcodes.ASM7;
    private final AnnotationVisitor annotationVisitor;
    private final MethodVisitor methodVisitor;
    private final FieldVisitor fieldVisitor;

    private final Set<String> descriptors = new HashSet<>();

    DependenciesCollector()
    {
        super( API_VERSION );
        this.annotationVisitor = new AnnotationClassCollectorVisitor( API_VERSION, descriptors );
        this.methodVisitor = new MethodClassCollectorVisitor( API_VERSION, descriptors, annotationVisitor );
        this.fieldVisitor = new FieldClassCollectorVisitor( API_VERSION, descriptors, annotationVisitor );
    }

    @Override
    public AnnotationVisitor visitAnnotation( String descriptor, boolean visible )
    {
        descriptors.add( descriptor );
        return annotationVisitor;
    }

    @Override
    public AnnotationVisitor visitTypeAnnotation( int typeRef, TypePath typePath, String descriptor, boolean visible )
    {
        descriptors.add( descriptor );
        return annotationVisitor;
    }

    @Override
    public FieldVisitor visitField( int access, String name, String descriptor, String signature, Object value )
    {
        descriptors.add( descriptor );
        return fieldVisitor;
    }

    @Override
    public MethodVisitor visitMethod( int access, String name, String descriptor, String signature, String[] exceptions )
    {
        return methodVisitor;
    }

    Set<String> getJunitTestClasses()
    {
        return descriptors.stream()
                .filter( d -> d.startsWith( "org/junit" ) || d.startsWith( "Lorg/junit" ) )
                .map( d -> d.startsWith( "o" ) ? getObjectType( d ) : getType( d ) )
                .map( Type::getClassName )
                .collect( Collectors.toSet() );
    }
}
