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
package org.neo4j.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import org.neo4j.kernel.impl.annotations.Documented;

@Ignore( "this is not a test, it is a testing utility" )
public class TestData<T> implements TestRule
{
    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    public @interface Title
    {
        String value();
    }

    public interface Producer<T>
    {
        T create( GraphDefinition graph, String title, String documentation );

        void destroy( T product, boolean successful );
    }

    public static <T> TestData<T> producedThrough( Producer<T> transformation )
    {
        transformation.getClass(); // null check
        return new TestData<T>( transformation );
    }

    public T get()
    {
        return get( true );
    }

    private static final class Lazy
    {
        private volatile Object productOrFactory;

        Lazy( GraphDefinition graph, String title, String documentation )
        {
            productOrFactory = new Factory( graph, title, documentation );
        }

        @SuppressWarnings( "unchecked"/*cast to T*/)
        <T> T get( Producer<T> producer, boolean create )
        {
            Object result = productOrFactory;
            if ( result instanceof Factory )
            {
                synchronized ( this )
                {
                    if ( ( result = productOrFactory ) instanceof Factory )
                    {
                        productOrFactory = result = ( (Factory) result ).create( producer, create );
                    }
                }
            }
            return (T) result;
        }
    }

    private static final class Factory
    {
        private final GraphDefinition graph;
        private final String title;
        private final String documentation;

        Factory( GraphDefinition graph, String title, String documentation )
        {
            this.graph = graph;
            this.title = title;
            this.documentation = documentation;
        }

        Object create( Producer<?> producer, boolean create )
        {
            return create ? producer.create( graph, title, documentation ) : null;
        }
    }

    private final Producer<T> producer;
    private final ThreadLocal<Lazy> product = new InheritableThreadLocal<Lazy>();

    private TestData( Producer<T> producer )
    {
        this.producer = producer;
    }
    
    @Override
    public Statement apply( final Statement base, final Description description )
    {
        final Title title = description.getAnnotation( Title.class );
        final Documented doc = description.getAnnotation( Documented.class );
        GraphDescription.Graph g = description.getAnnotation( GraphDescription.Graph.class );
        if ( g == null ) g = description.getTestClass().getAnnotation( GraphDescription.Graph.class );
        final GraphDescription graph = GraphDescription.create( g );
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                product.set( create( graph, title == null ? null : title.value(), doc == null ? null : doc.value(),
                        description.getMethodName() ) );
                try
                {
                    try
                    {
                        base.evaluate();
                    }
                    catch ( Throwable err )
                    {
                        try
                        {
                            destroy( get( false ), false );
                        }
                        catch ( Throwable sub )
                        {
                            List<Throwable> failures = new ArrayList<Throwable>();
                            if ( err instanceof MultipleFailureException )
                            {
                                failures.addAll( ( (MultipleFailureException) err ).getFailures() );
                            }
                            else
                            {
                                failures.add( err );
                            }
                            failures.add( sub );
                            throw new MultipleFailureException( failures );
                        }
                        throw err;
                    }
                    destroy( get( false ), false );
                }
                finally
                {
                    product.set( null );
                }
            }
        };
    }

    private void destroy( @SuppressWarnings( "hiding" ) T product, boolean successful )
    {
        if ( product != null ) producer.destroy( product, successful );
    }

    private T get( boolean create )
    {
        Lazy lazy = product.get();
        if ( lazy == null )
        {
            if ( create ) throw new IllegalStateException( "Not in test case" );
            return null;
        }
        return lazy.get( producer, create );
    }

    private static final String EMPTY = "";

    private static Lazy create( GraphDescription graph, String title, String doc, String methodName )
    {
        if ( doc != null )
        {
            if ( title == null )
            {
                // standard javadoc means of finding a title
                int dot = doc.indexOf( '.' );
                if ( dot > 0 )
                {
                    title = doc.substring( 0, dot );
                    if ( title.contains( "\n" ) )
                    {
                        title = null;
                    }
                    else
                    {
                        title = title.trim();
                        doc = doc.substring( dot + 1 );
                    }
                }
            }
            String[] lines = doc.split( "\n" );
            int indent = Integer.MAX_VALUE;
            int start = 0, end = 0;
            for ( int i = 0; i < lines.length; i++ )
            {
                if ( EMPTY.equals( lines[i].trim() ) )
                {
                    lines[i] = EMPTY;
                    if ( start == i ) end = ++start; // skip initial blank lines
                }
                else
                {
                    for ( int j = 0; j < lines[i].length(); j++ )
                    {
                        if ( !Character.isWhitespace( lines[i].charAt( j ) ) )
                        {
                            indent = Math.min( indent, j );
                            break;
                        }
                    }
                    end = i; // skip blank lines at the end
                }
            }
            if ( end == lines.length ) end--; // all lines were empty
            // If there still is no title, and the first line looks like a
            // title, take the first line as title
            if ( title == null && start < end && EMPTY.equals( lines[start + 1] ) )
            {
                title = lines[start].trim();
                start += 2;
            }
            StringBuilder documentation = new StringBuilder();
            for ( int i = start; i <= end; i++ )
            {
                documentation.append( EMPTY.equals( lines[i] ) ? EMPTY : lines[i].substring( indent ) ).append( "\n" );
            }
            doc = documentation.toString();
        }
        else
        {
            doc = EMPTY;
        }
        if ( title == null )
        {
            title = methodName.replace( "_", " " );
        }
        return new Lazy( graph, title, doc );
    }
}
