/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.processing.Processor;
import javax.tools.JavaCompiler;

import org.neo4j.codegen.TypeReference;

class Configuration
{
    private List<Processor> processors;
    private Set<SourceCode> flags;
    private List<String> options;
    private List<SourceVisitor> sourceVisitors;
    private List<WarningsHandler> warningsHandlers;

    public void withAnnotationProcessor( Processor processor )
    {
        if ( processors == null )
        {
            processors = new ArrayList<>();
        }
        processors.add( processor );
    }

    public void withFlag( SourceCode flag )
    {
        if ( flags == null )
        {
            flags = EnumSet.of( flag );
        }
        else
        {
            flags.add( flag );
        }
    }

    public Iterable<String> options()
    {
        return options;
    }

    public void processors( JavaCompiler.CompilationTask task )
    {
        if ( processors != null )
        {
            task.setProcessors( processors );
        }
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

    public void addSourceVisitor( SourceVisitor visitor )
    {
        if ( sourceVisitors == null )
        {
            sourceVisitors = new ArrayList<>();
        }
        sourceVisitors.add( visitor );
    }

    public void visit( TypeReference reference, StringBuilder source )
    {
        if ( sourceVisitors != null )
        {
            for ( SourceVisitor visitor : sourceVisitors )
            {
                visitor.visitSource( reference, source );
            }
        }
    }

    public void addWarningsHandler( WarningsHandler handler )
    {
        if ( warningsHandlers == null )
        {
            warningsHandlers = new ArrayList<>();
        }
        warningsHandlers.add( handler );
    }

    public WarningsHandler warningsHandler()
    {
        if ( warningsHandlers == null )
        {
            return WarningsHandler.NO_WARNINGS_HANDLER;
        }
        if ( warningsHandlers.size() == 1 )
        {
            return warningsHandlers.get( 0 );
        }
        else
        {
            return new WarningsHandler.Multiplex(
                    warningsHandlers.toArray( new WarningsHandler[warningsHandlers.size()] ) );
        }
    }
}
