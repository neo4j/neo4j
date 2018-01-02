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

import java.util.List;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.neo4j.codegen.ByteCodes;
import org.neo4j.codegen.CodeGenerationStrategy;
import org.neo4j.codegen.CodeGenerationStrategyNotSupportedException;
import org.neo4j.codegen.CompilationFailureException;

class JdkCompiler implements SourceCompiler
{
    public static final Factory FACTORY = new Factory()
    {
        @Override
        SourceCompiler sourceCompilerFor( Configuration configuration, CodeGenerationStrategy<?> strategy )
                throws CodeGenerationStrategyNotSupportedException
        {
            JavaCompiler jdkCompiler = ToolProvider.getSystemJavaCompiler();
            if ( jdkCompiler == null )
            {
                throw new CodeGenerationStrategyNotSupportedException( strategy, "no java source compiler available" );
            }
            return new JdkCompiler( jdkCompiler, configuration );
        }
    };
    private final JavaCompiler compiler;
    private final Configuration configuration;

    JdkCompiler( JavaCompiler compiler, Configuration configuration )
    {
        this.compiler = compiler;
        this.configuration = configuration;
    }

    @Override
    public Iterable<? extends ByteCodes> compile( List<JavaSourceFile> sourceFiles, ClassLoader loader )
            throws CompilationFailureException
    {
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

        FileManager fileManager = new FileManager(
                compiler.getStandardFileManager( diagnostics, configuration.locale(), configuration.chraset() ) );

        JavaCompiler.CompilationTask task = compiler.getTask(
                configuration.errorWriter(), fileManager, diagnostics, configuration.options(), null, sourceFiles );

        configuration.processors( task );
        if ( task.call() )
        {
            configuration.warningsHandler().handle( diagnostics.getDiagnostics() );
            return fileManager.bytecodes();
        }
        else
        {
            @SuppressWarnings( "unchecked" )
            List<Diagnostic<?>> issues = (List) diagnostics.getDiagnostics();
            throw new CompilationFailureException( issues );
        }
    }
}
