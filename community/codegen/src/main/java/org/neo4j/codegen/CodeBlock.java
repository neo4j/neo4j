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

import static org.neo4j.codegen.Resource.withResource;
import static org.neo4j.codegen.TypeReference.typeReference;

public class CodeBlock implements AutoCloseable
{
    final ClassGenerator clazz;
    private MethodEmitter emitter;
    private final CodeBlock parent;
    private boolean done;

    CodeBlock( CodeBlock parent )
    {
        this.clazz = parent.clazz;
        this.emitter = parent.emitter;
        parent.emitter = InvalidState.IN_SUB_BLOCK;
        this.parent = parent;
    }

    CodeBlock( ClassGenerator clazz, MethodEmitter emitter )
    {
        this.clazz = clazz;
        this.emitter = emitter;
        this.parent = null;
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

    private void endBlock()
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

    TypeReference local( String name )
    {
        return null;
    }

    public LocalVariable declare( TypeReference type, String name )
    {
        LocalVariable local = new LocalVariable( type, name );
        emitter.declare( local );
        return local;
    }

    public void assign( LocalVariable local, Expression value )
    {
        emitter.assign( local, value );
    }

    public void assign( Class<?> type, String name, Expression value )
    {
        assign( typeReference( type ), name, value );
    }

    public void assign( TypeReference type, String name, Expression value )
    {
        emitter.assign( type, name, value );
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
        return Expression.load( local( name ), name );
    }

    public CodeBlock forEach( Parameter local, Expression iterable )
    {
        emitter.beginForEach( local, iterable );
        return new CodeBlock( this );
    }

    public CodeBlock whileLoop( Expression test )
    {
        emitter.beginWhile( test );
        return new CodeBlock( this );
    }

    public CodeBlock ifStatement( Expression test )
    {
        emitter.beginIf( test );
        return new CodeBlock( this );
    }

    CodeBlock emitCatch( Parameter exception )
    {
        endBlock();
        emitter.beginCatch( exception );
        return new CodeBlock( this );
    }

    CodeBlock emitFinally()
    {
        endBlock();
        emitter.beginFinally();
        return new CodeBlock( this );
    }

    public CodeBlock tryBlock( Class<?> resourceType, String resourceName, Expression resource )
    {
        return tryBlock( withResource( resourceType, resourceName, resource ) );
    }

    public CodeBlock tryBlock( TypeReference resourceType, String resourceName, Expression resource )
    {
        return tryBlock( withResource( resourceType, resourceName, resource ) );
    }

    public TryBlock tryBlock( Resource... resources )
    {
        emitter.beginTry( resources );
        return new TryBlock( this );
    }

    public void returns()
    {
        emitter.returns();
    }

    public void returns( Expression value )
    {
        emitter.returns( value );
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
