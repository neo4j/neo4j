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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import static java.util.Collections.unmodifiableList;

public class CompilationFailureException extends Exception
{
    private final List<? extends Diagnostic<?>> diagnostics;

    public CompilationFailureException( List<? extends Diagnostic<?>> diagnostics )
    {
        super( String.format( "Compilation failed with %d reported issues.%s",
                diagnostics.size(), source( diagnostics ) ) );
        this.diagnostics = diagnostics;
    }

    @Override
    public String toString()
    {
        StringWriter result = new StringWriter().append( super.toString() );
        for ( Diagnostic<?> diagnostic : diagnostics )
        {
            format( result.append( "\n\t\t" ), diagnostic );
        }
        return result.toString();
    }

    private static String source( List<? extends Diagnostic<?>> diagnostics )
    {
        Set<JavaFileObject> sources = null;
        for ( Diagnostic<?> diagnostic : diagnostics )
        {
            Object source = diagnostic.getSource();
            if ( source instanceof JavaFileObject )
            {
                JavaFileObject file = (JavaFileObject) source;
                if ( file.getKind() == JavaFileObject.Kind.SOURCE )
                {
                    if ( sources == null )
                    {
                        sources = Collections.newSetFromMap( new IdentityHashMap<JavaFileObject,Boolean>() );
                    }
                    sources.add( file );
                }
            }
        }
        if ( sources == null )
        {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for ( JavaFileObject source : sources )
        {
            int pos = result.length();
            result.append( "\nSource file " ).append( source.getName() ).append( ":\n" );
            try
            {
                CharSequence content = source.getCharContent( true );
                result.append( String.format( "%4d: ", 1 ) );
                for ( int line = 1, i = 0; i < content.length(); i++ )
                {
                    char c = content.charAt( i );
                    result.append( c );
                    if ( c == '\n' )
                    {
                        result.append( String.format( "%4d: ", ++line ) );
                    }
                }
            }
            catch ( IOException e )
            {
                result.setLength( pos );
            }
        }
        return result.toString();
    }

    public static void format( Appendable result, Diagnostic<?> diagnostic )
    {
        try
        {
            Object source = diagnostic.getSource();
            if ( source != null )
            {
                result.append( diagnostic.getKind().name() )
                        .append( " on line " ).append( Long.toString( diagnostic.getLineNumber() ) )
                        .append( " in " ).append( source.toString() )
                        .append( ": " ).append( diagnostic.getMessage( null ) );
            }
            else
            {
                result.append( diagnostic.getMessage( null ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to append.", e );
        }
    }

    public List<Diagnostic<?>> getDiagnostics()
    {
        return unmodifiableList( diagnostics );
    }
}
