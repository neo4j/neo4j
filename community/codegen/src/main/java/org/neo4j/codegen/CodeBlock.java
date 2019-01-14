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
package org.neo4j.codegen;

import java.util.Iterator;
import java.util.function.Consumer;

import static org.neo4j.codegen.LocalVariables.copy;
import static org.neo4j.codegen.MethodReference.methodReference;
import static org.neo4j.codegen.TypeReference.typeReference;

public class CodeBlock implements AutoCloseable
{

    final ClassGenerator clazz;
    private MethodEmitter emitter;
    private final CodeBlock parent;
    private boolean done;
    private boolean continuableBlock;

    protected LocalVariables localVariables = new LocalVariables();

    CodeBlock( CodeBlock parent )
    {
        this( parent, parent.continuableBlock );
    }

    CodeBlock( CodeBlock parent, boolean continuableBlock )
    {
        this.clazz = parent.clazz;
        this.emitter = parent.emitter;
        parent.emitter = InvalidState.IN_SUB_BLOCK;
        this.parent = parent;
        //copy over local variables from parent
        this.localVariables = copy( parent.localVariables );
        this.continuableBlock = continuableBlock;
    }

    CodeBlock( ClassGenerator clazz, MethodEmitter emitter, Parameter... parameters )
    {
        this.clazz = clazz;
        this.emitter = emitter;
        this.parent = null;
        this.continuableBlock = false;
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

    public CodeBlock parent()
    {
        return parent;
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

    public void expression( Expression expression )
    {
        emitter.expression( expression );
    }

    LocalVariable local( String name )
    {
        return localVariables.get( name );
    }

    public LocalVariable declare( TypeReference type, String name )
    {
        LocalVariable local = localVariables.createNew( type, name );
        emitter.declare( local );
        return local;
    }

    public void assign( LocalVariable local, Expression value )
    {
        emitter.assignVariableInScope( local, value );
    }

    public void assign( Class<?> type, String name, Expression value )
    {
        assign( typeReference( type ), name, value );
    }

    public void assign( TypeReference type, String name, Expression value )
    {
        LocalVariable variable = localVariables.createNew( type, name );
        emitter.assign( variable, value );
    }

    public void put( Expression target, FieldReference field, Expression value )
    {
        emitter.put( target, field, value );
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

        assign( Iterator.class, iteratorName, Expression.invoke( iterable,
                MethodReference.methodReference( Iterable.class, Iterator.class, "iterator" ) ) );
        CodeBlock block = whileLoop( Expression
                .invoke( load( iteratorName ), methodReference( Iterator.class, boolean.class, "hasNext" ) ) );
        block.assign( local.type(), local.name(),
                Expression.cast( local.type(), Expression.invoke( block.load( iteratorName ),
                        methodReference( Iterator.class, Object.class, "next" ) ) ) );

        return block;
    }

    public CodeBlock whileLoop( Expression test )
    {
        emitter.beginWhile( test );
        return new CodeBlock( this, true );
    }

    public CodeBlock ifStatement( Expression test )
    {
        emitter.beginIf( test );
        return new CodeBlock( this );
    }

    public CodeBlock block()
    {
        emitter.beginBlock();
        return new CodeBlock( this );
    }

    public void tryCatch( Consumer<CodeBlock> body, Consumer<CodeBlock> onError, Parameter exception )
    {
        emitter.tryCatchBlock( body, onError, localVariables.createNew( exception.type(), exception.name() ), this );
    }

    public void returns()
    {
        emitter.returns();
    }

    public void returns( Expression value )
    {
        emitter.returns( value );
    }

    public void continueIfPossible()
    {
        if ( continuableBlock )
        {
            emitter.continues();
        }
    }

    public void throwException( Expression exception )
    {
        emitter.throwException( exception );
    }

    public TypeReference owner()
    {
        return clazz.handle();
    }
}
