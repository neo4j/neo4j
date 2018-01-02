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
package org.neo4j.codegen.source;

import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.processing.Processor;
import javax.tools.JavaCompiler;

import org.neo4j.codegen.CodeGenerationStrategy;
import org.neo4j.codegen.CodeGenerationStrategyNotSupportedException;
import org.neo4j.codegen.TypeReference;

class Configuration
{
    private List<Processor> processors = new ArrayList<>();
    private Set<SourceCode> flags = EnumSet.noneOf( SourceCode.class );
    private List<String> options = new ArrayList<>();
    private List<SourceVisitor> sourceVisitors = new ArrayList<>();
    private List<WarningsHandler> warningsHandlers = new ArrayList<>();
    SourceCompiler.Factory compiler = JdkCompiler.FACTORY;

    public Configuration withAnnotationProcessor( Processor processor )
    {
        processors.add( processor );
        return this;
    }

    public Configuration withFlag( SourceCode flag )
    {
        flags.add( flag );
        return this;
    }

    public Configuration withOptions( String... opts )
    {
        if ( opts != null )
        {
            Collections.addAll( options, opts );
        }
        return this;
    }

    public Configuration withSourceVisitor( SourceVisitor visitor )
    {
        sourceVisitors.add( visitor );
        return this;
    }

    public Configuration withWarningsHandler( WarningsHandler handler )
    {
        warningsHandlers.add( handler );
        return this;
    }

    public Iterable<String> options()
    {
        return options;
    }

    public void processors( JavaCompiler.CompilationTask task )
    {
        task.setProcessors( processors );
    }

    public Locale locale()
    {
        return null;
    }

    public Charset chraset()
    {
        return null;
    }

    public Writer errorWriter()
    {
        return null;
    }

    public BaseUri sourceBase()
    {
        return BaseUri.DEFAULT_SOURCE_BASE;
    }

    public boolean isSet( SourceCode flag )
    {
        return flags != null && flags.contains( flag );
    }

    public void visit( TypeReference reference, StringBuilder source )
    {
        for ( SourceVisitor visitor : sourceVisitors )
        {
            visitor.visitSource( reference, source );
        }
    }

    public WarningsHandler warningsHandler()
    {
        if ( warningsHandlers.isEmpty() )
        {
            return WarningsHandler.NO_WARNINGS_HANDLER;
        }
        if ( warningsHandlers.size() == 1 )
        {
            return warningsHandlers.get( 0 );
        }
        return new WarningsHandler.Multiplex(
                warningsHandlers.toArray( new WarningsHandler[warningsHandlers.size()] ) );
    }

    public SourceCompiler sourceCompilerFor( CodeGenerationStrategy<?> strategy )
            throws CodeGenerationStrategyNotSupportedException
    {
        return compiler.sourceCompilerFor( this, strategy );
    }

    public void useJdkJavaCompiler()
    {
        compiler = null;
    }
}
