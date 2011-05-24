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
package org.neo4j.server.rest;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.neo4j.kernel.impl.annotations.Documented;

public class DocumentationGenerator implements MethodRule
{
    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    public @interface Title
    {
        String value();
    }

    private static final String EMPTY = "";

    private String documentation = null, title = null;

    @Override
    public Statement apply( Statement base, FrameworkMethod method, Object target )
    {
        Title title = method.getAnnotation( Title.class );
        Documented documentation = method.getAnnotation( Documented.class );
        if ( title == null && documentation != null )
        {
            String docString = documentation.value();
            int pos = docString.indexOf( "." );
            if ( pos > 1 )
            {
                this.title = docString.substring( 1, pos );
                this.documentation = deIndent( docString.substring( pos + 1 ) );
                return base;
            }
        }
        this.title = title != null ? title.value() : method.getName();
        if ( documentation != null )
            this.documentation = documentation.value();
        return base;
    }

    public DocsGenerator create()
    {
        DocsGenerator result = DocsGenerator.create( title );
        if ( documentation != null )
        {
            return result.description( deIndent( documentation ) );
        }
        return result;
    }

    private String deIndent( String descr )
    {
        String[] lines = descr.split( "\n" );
        int indent = Integer.MAX_VALUE;
        for ( int i = 0; i < lines.length; i++ )
        {
            if ( EMPTY.equals( lines[i].trim() ) )
            {
                lines[i] = EMPTY;
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
            }
        }
        StringBuilder result = new StringBuilder();
        for ( String line : lines )
        {
            result.append( line == EMPTY ? line : line.substring( indent ) ).append( "\n" );
        }
        return result.toString();
    }
}