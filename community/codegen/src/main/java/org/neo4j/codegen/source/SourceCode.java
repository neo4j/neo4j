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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import javax.annotation.processing.Processor;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import org.neo4j.codegen.TypeReference;
import org.neo4j.codegen.CodeGenerationStrategy;
import org.neo4j.codegen.CodeGenerator;
import org.neo4j.codegen.CodeGeneratorOption;

import static java.util.Objects.requireNonNull;

import static org.neo4j.codegen.CompilationFailureException.format;

public enum SourceCode implements CodeGeneratorOption
{
    ;
    public static final CodeGeneratorOption SOURCECODE = new CodeGenerationStrategy<Configuration>()
    {
        @Override
        protected Configuration createConfigurator()
        {
            return new Configuration();
        }

        @Override
        protected CodeGenerator createCodeGenerator( ClassLoader loader, Configuration configuration )
        {
            return new SourceCodeGenerator( loader, configuration );
        }

        @Override
        public String toString()
        {
            return "SourceCode";
        }
    };
    public static final CodeGeneratorOption PRINT_SOURCE = new SourceVisitor()
    {
        @Override
        protected void visitSource( TypeReference reference, CharSequence sourceCode )
        {
            System.out.println( "=== Generated class " + reference.name() + " ===\n" + sourceCode );
        }

        @Override
        public String toString()
        {
            return "PRINT_SOURCE";
        }
    };
    public static final CodeGeneratorOption PRINT_WARNINGS = printWarningsTo( System.err );

    private static CodeGeneratorOption printWarningsTo( PrintStream err )
    {
        return new MyCodeGeneratorOption( err );
    }

    public static CodeGeneratorOption annotationProcessor( Processor processor )
    {
        return new AnnotationProcessorOption( requireNonNull( processor ) );
    }

    public static CodeGeneratorOption sourceLocation( Path path )
    {
        return new SourceLocationOption( requireNonNull( path ) );
    }

    public static CodeGeneratorOption temporarySourceCodeLocation()
    {
        try
        {
            return new SourceLocationOption( Files.createTempDirectory( null ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to create temporary directory.", e );
        }
    }

    @Override
    public void applyTo( Object target )
    {
        if ( target instanceof Configuration )
        {
            ((Configuration) target).withFlag( this );
        }
    }

    private static class AnnotationProcessorOption implements CodeGeneratorOption
    {
        private final Processor processor;

        AnnotationProcessorOption( Processor processor )
        {
            this.processor = processor;
        }

        @Override
        public void applyTo( Object target )
        {
            if ( target instanceof Configuration )
            {
                ((Configuration) target).withAnnotationProcessor( processor );
            }
        }

        @Override
        public String toString()
        {
            return "annotationProcessor( " + processor + " )";
        }
    }

    private static class SourceLocationOption extends SourceVisitor
    {
        private final Path path;

        SourceLocationOption( Path path )
        {
            this.path = path;
        }

        @Override
        public String toString()
        {
            return "sourceLocation( " + path + " )";
        }

        @Override
        protected void visitSource( TypeReference reference, CharSequence sourceCode )
        {
            try
            {
                Path location = path( reference );
                Files.createDirectories( location.getParent() );
                Files.write( location, Collections.singletonList( sourceCode ), Charset.defaultCharset() );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Failed to write source code", e );
            }
        }

        private Path path( TypeReference reference )
        {
            return path.resolve( reference.packageName().replace( '.', '/' ) + "/" + reference.simpleName() + ".java" );
        }
    }

    private static class MyCodeGeneratorOption implements CodeGeneratorOption, WarningsHandler
    {
        private final PrintStream target;

        MyCodeGeneratorOption( PrintStream target )
        {
            this.target = target;
        }

        @Override
        public void applyTo( Object target )
        {
            if ( target instanceof Configuration )
            {
                ((Configuration) target).addWarningsHandler( this );
            }
        }

        @Override
        public String toString()
        {
            return "PRINT_WARNINGS";
        }

        @Override
        public void handle( List<Diagnostic<? extends JavaFileObject>> diagnostics )
        {
            for ( Diagnostic<? extends JavaFileObject> diagnostic : diagnostics )
            {
                format( target, diagnostic );
            }
        }
    }
}
