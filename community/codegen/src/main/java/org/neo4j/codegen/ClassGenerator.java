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

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.neo4j.codegen.MethodDeclaration.TypeParameter.NO_PARAMETERS;
import static org.neo4j.codegen.MethodDeclaration.constructor;
import static org.neo4j.codegen.MethodDeclaration.method;
import static org.neo4j.codegen.MethodReference.methodReference;
import static org.neo4j.codegen.TypeReference.NO_TYPES;
import static org.neo4j.codegen.TypeReference.typeReference;

public class ClassGenerator implements AutoCloseable
{
    private final ClassHandle handle;
    private ClassEmitter emitter;
    private Map<String,FieldReference> fields;

    ClassGenerator( ClassHandle handle, ClassEmitter emitter )
    {
        this.handle = handle;
        this.emitter = emitter;
    }

    @Override
    public void close()
    {
        emitter.done();
        handle.generator.closeClass();
        emitter = InvalidState.CLASS_DONE;
    }

    public ClassHandle handle()
    {
        return handle;
    }

    public FieldReference field( Class<?> type, String name )
    {
        return field( typeReference( type ), name );
    }

    public FieldReference field( TypeReference type, String name )
    {
        return emitField( Modifier.PUBLIC, type, name, null );
    }

    public FieldReference staticField( Class<?> type, String name, Expression value )
    {
        return staticField( typeReference( type ), name, value );
    }

    public FieldReference staticField( TypeReference type, String name )
    {
        return emitField( Modifier.PUBLIC | Modifier.STATIC, type, name, null );
    }

    public FieldReference staticField( TypeReference type, String name, Expression value )
    {
        return emitField( Modifier.PRIVATE | Modifier.STATIC | Modifier.FINAL, type, name, Objects.requireNonNull( value ) );
    }

    private FieldReference emitField( int modifiers, TypeReference type, String name, Expression value )
    {
        if ( fields == null )
        {
            fields = new HashMap<>();
        }
        else if ( fields.containsKey( name ) )
        {
            throw new IllegalArgumentException( handle + " already has a field '" + name + "'" );
        }
        FieldReference field = new FieldReference( modifiers, handle, type, name );
        fields.put( name, field );
        emitter.field( field, value );
        return field;
    }

    public MethodReference generate( MethodTemplate template, Binding... bindings )
    {
        try ( CodeBlock generator = generate( template.declaration( handle ) ) )
        {
            template.generate( generator );
        }
        return methodReference( handle, template.returnType(), template.name(), template.parameterTypes() );
    }

    public CodeBlock generateConstructor( Parameter... parameters )
    {
        return generate( constructor( handle, parameters,/*throws:*/NO_TYPES, NO_PARAMETERS ) );
    }

    public CodeBlock generateMethod( Class<?> returnType, String name, Parameter... parameters )
    {
        return generateMethod( typeReference( returnType ), name, parameters );
    }

    public CodeBlock generateMethod( TypeReference returnType, String name, Parameter... parameters )
    {
        return generate( method( handle, returnType, name, parameters,/*throws:*/NO_TYPES, NO_PARAMETERS ) );
    }

    public CodeBlock generate( MethodDeclaration.Builder builder )
    {
        return generate( builder.build( handle ) );
    }

    private CodeBlock generate( MethodDeclaration declaration )
    {
        return new CodeBlock( this, emitter.method( declaration ) );
    }

    FieldReference getField( String name )
    {
        return fields == null ? null : fields.get( name );
    }
}
