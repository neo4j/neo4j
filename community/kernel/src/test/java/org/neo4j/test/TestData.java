/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.junit.internal.runners.model.MultipleFailureException;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.neo4j.kernel.impl.annotations.Documented;

@Ignore( "this is not a test, it is a testing utility" )
public class TestData<T> implements MethodRule
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

        void destroy( T product );
    }

    public static <T> TestData<T> producedThrough( Producer<T> transformation )
    {
        transformation.getClass();
        return new TestData<T>( transformation );
    }

    private final Producer<T> transformation;
    private T product;

    private TestData( Producer<T> transformation )
    {
        this.transformation = transformation;
    }

    @Override
    public Statement apply( final Statement base, final FrameworkMethod method, Object target )
    {
        final Title title = method.getAnnotation( Title.class );
        final Documented doc = method.getAnnotation( Documented.class );
        final GraphDescription graph = GraphDescription.create( method.getAnnotation( GraphDescription.Graph.class ) );
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                product = create( graph, title == null ? null : title.value(), doc == null ? null : doc.value(),
                        method.getName() );
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
                            transformation.destroy( product );
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
                    transformation.destroy( product );
                }
                finally
                {
                    product = null;
                }
            }
        };
    }

    private static final String EMPTY = "";

    private T create( GraphDescription graph, String title, String doc, String methodName )
    {
        if ( doc != null )
        {
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
            // If there is no title, and the first line looks like a title,
            // take the first line as title
            if ( title == null && lines[start + 1] == EMPTY )
            {
                title = lines[start].trim();
                start += 2;
            }
            StringBuilder documentation = new StringBuilder();
            for ( int i = start; i < end; i++ )
            {
                documentation.append( lines[i] == EMPTY ? lines[i] : lines[i].substring( indent ) ).append( "\n" );
            }
            doc = documentation.toString();
        }
        else
        {
            doc = EMPTY;
        }
        if ( title == null )
        {
            title = methodName;
        }
        return transformation.create( graph, title, doc );
    }

    public T get()
    {
        return product;
    }
}
