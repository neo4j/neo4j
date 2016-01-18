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
package org.neo4j.codegen;

import java.util.Iterator;
import java.util.function.Consumer;

import static org.neo4j.codegen.LocalVariables.copy;
import static org.neo4j.codegen.MethodReference.methodReference;
import static org.neo4j.codegen.Resource.withResource;
import static org.neo4j.codegen.TypeReference.typeReference;

public class CodeBlock implements AutoCloseable
{

    final ClassGenerator clazz;
    private MethodEmitter emitter;
    private final CodeBlock parent;
    private boolean done;

    protected LocalVariables localVariables = new LocalVariables();

    CodeBlock( CodeBlock parent )
    {
        this.clazz = parent.clazz;
        this.emitter = parent.emitter;
        parent.emitter = InvalidState.IN_SUB_BLOCK;
        this.parent = parent;
        //copy over local variables from parent
        this.localVariables = copy(parent.localVariables);
    }

    CodeBlock( ClassGenerator clazz, MethodEmitter emitter, Parameter...parameters )
    {
        this.clazz = clazz;
        this.emitter = emitter;
        this.parent = null;
        localVariables.createNew( clazz.handle(), "this" );
        for ( Parameter parameter : parameters )
        {
            localVariables.createNew( parameter.type(), parameter.name() );
        }
    }

    public ClassGenerator classGenerator()
    {
        return clazz;
    }

    @Override
    public void close()
    {
        endBlock();
        if ( parent != null )
        {
            parent.emitter = emitter;
        }
        else
        {
            emitter.done();
        }
        this.emitter = InvalidState.BLOCK_CLOSED;
    }

    protected void endBlock()
    {
        if ( !done )
        {
            emitter.endBlock();
            done = true;
        }
    }

    protected void emit( Consumer<MethodEmitter> emitFunction )
    {
        emitFunction.accept( emitter );
    }

    public void expression( Expression expression )
    {
        emit( ( e ) -> e.expression( expression ) );
    }

    LocalVariable local( String name )
    {
        return localVariables.get( name);
    }

    public LocalVariable declare( TypeReference type, String name )
    {
        LocalVariable local = localVariables.createNew( type, name );
        emit( e -> e.declare( local ) );
        return local;
    }

    public void assign( LocalVariable local, Expression value )
    {
        emit( e -> e.assignVariableInScope( local, value ) );
    }

    public void assign( Class<?> type, String name, Expression value )
    {
        assign( typeReference( type ), name, value );
    }

    public void assign( TypeReference type, String name, Expression value )
    {
        LocalVariable variable = localVariables.createNew( type, name );
        emit( e -> e.assign( variable, value ) );
    }

    public void put( Expression target, FieldReference field, Expression value )
    {
        emit( e -> e.put( target, field, value ) );
    }

    public Expression self()
    {
        return load( "this" );
    }

    public Expression load( String name )
    {
        return Expression.load( local( name ) );
    }

    /*
     * Foreach is just syntactic sugar for a while loop.
     *
     */
    public CodeBlock forEach( Parameter local, Expression iterable )
    {
        String iteratorName = local.name() + "Iter";

        try
        {
            assign( Iterator.class, iteratorName, Expression.invoke( iterable,
                    MethodReference.methodReference( Iterable.class, Iterator.class, "iterator" ) ));
            CodeBlock block = whileLoop( Expression
                    .invoke( load( iteratorName ), methodReference( Iterator.class, boolean.class, "hasNext" ) ) );
            block.assign( local.type(), local.name(),
                    Expression.cast(local.type(), Expression.invoke( block.load( iteratorName ),
                            methodReference( Iterator.class, Object.class, "next" ))) );

            return block;
        }
        catch ( NoSuchMethodException e )
        {
            throw new RuntimeException( e );
        }
    }

    public CodeBlock whileLoop( Expression test )
    {
        emit( e -> e.beginWhile( test ) );
        return new CodeBlock( this );
    }

    public CodeBlock ifStatement( Expression test )
    {
        emit( e -> e.beginIf( test ) );
        return new CodeBlock( this );
    }

    public TryBlock tryBlock( Class<?> resourceType, String resourceName, Expression resource )
    {
        return tryBlock( withResource( resourceType, resourceName, resource ) );
    }

    public TryBlock tryBlock( TypeReference resourceType, String resourceName, Expression resource )
    {
        return tryBlock( withResource( resourceType, resourceName, resource ) );
    }

    public TryBlock tryBlock( Resource... resources )
    {
        return new TryBlock( this, resources );
    }

    public void returns()
    {
        emit( MethodEmitter::returns );
    }

    public void returns( Expression value )
    {
        emit( e -> e.returns( value ) );
    }

    public void throwException( Expression exception )
    {
        emit( e -> e.throwException( exception ) );
    }

    public TypeReference owner()
    {
        return clazz.handle();
    }

}
