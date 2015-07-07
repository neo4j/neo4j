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

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.ClassLoaderIClassLoader;
import org.codehaus.janino.Descriptor;
import org.codehaus.janino.IClass;
import org.codehaus.janino.IClassLoader;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.UnitCompiler;
import org.codehaus.janino.util.ClassFile;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import javax.tools.Diagnostic;

import org.neo4j.codegen.ByteCodes;
import org.neo4j.codegen.CodeGenerationStrategy;
import org.neo4j.codegen.CodeGenerationStrategyNotSupportedException;
import org.neo4j.codegen.CompilationFailureException;

enum JaninoCompiler implements SourceCompiler
{
    INSTANCE;
    public static final Factory FACTORY = new Factory()
    {
        @Override
        SourceCompiler sourceCompilerFor( Configuration configuration, CodeGenerationStrategy<?> strategy )
                throws CodeGenerationStrategyNotSupportedException
        {
            return INSTANCE;
        }

        @Override
        void configure( Configuration configuration )
        {
            configuration.withFlag( SourceCode.SIMPLIFY_TRY_WITH_RESOURCE );
        }
    };

    @Override
    public Iterable<? extends ByteCodes> compile( List<JavaSourceFile> sourceFiles, ClassLoader classLoader )
            throws CompilationFailureException
    {
        Compiler compiler = new Compiler( new ClassLoaderIClassLoader( classLoader ) );
        List<Failure> failures = new ArrayList<>();
        for ( JavaSourceFile sourceFile : sourceFiles )
        {
            try
            {
                compiler.parse( sourceFile );
            }
            catch ( CompileException e )
            {
                failures.add( new Failure( e, sourceFile, e.getLocation() ) );
            }
            catch ( IOException e )
            {
                failures.add( new Failure( e, sourceFile, null ) );
            }
        }
        if ( !failures.isEmpty() )
        {
            throw new CompilationFailureException( failures );
        }
        List<ClassFile> classes = compiler.compile( sourceFiles );
        CompiledByteCodes[] result = new CompiledByteCodes[classes.size()];
        for ( int i = 0; i < result.length; i++ )
        {
            result[i] = new CompiledByteCodes( classes.get( i ) );
        }
        return Arrays.asList( result );
    }

    private static final class Compiler extends IClassLoader
    {
        private final List<UnitCompiler> units = new ArrayList<>();

        Compiler( IClassLoader parent )
        {
            super( parent );
            postConstruct();
        }

        void parse( JavaSourceFile sourceFile ) throws IOException, CompileException
        {
            Scanner scanner = new Scanner( sourceFile.getName(), new SourceReader( sourceFile ) );
            Java.CompilationUnit unit = new Parser( scanner ).parseCompilationUnit();
            units.add( new UnitCompiler( unit, this ) );
        }

        List<ClassFile> compile( List<JavaSourceFile> files ) throws CompilationFailureException
        {
            List<Failure> failures = new ArrayList<>();
            List<ClassFile> result = new ArrayList<>();
            for ( UnitCompiler unit : units )
            {
                try
                {
                    Collections.addAll( result, unit.compileUnit( true, true, true ) );
                }
                catch ( CompileException e )
                {
                    failures.add( new Failure( e, source( files, e.getLocation() ), e.getLocation() ) );
                }
            }
            if ( !failures.isEmpty() )
            {
                throw new CompilationFailureException( failures );
            }
            return result;
        }

        @Override
        protected IClass findIClass( String descriptor ) throws ClassNotFoundException
        {
            String className = Descriptor.toClassName( descriptor );

            // Determine the name of the top-level class.
            String topLevelClassName;
            {
                int idx = className.indexOf( '$' );
                topLevelClassName = idx == -1 ? className : className.substring( 0, idx );
            }

            // Check the parsed compilation units.
            for ( UnitCompiler uc : units )
            {
                IClass res = uc.findClass( topLevelClassName );
                if ( res != null )
                {
                    if ( !className.equals( topLevelClassName ) )
                    {
                        res = uc.findClass( className );
                        if ( res == null )
                        {
                            return null;
                        }
                    }
                    this.defineIClass( res );
                    return res;
                }
            }
            return null;
        }
    }

    private static JavaSourceFile source( List<JavaSourceFile> sourceFiles, Location location )
    {
        JavaSourceFile source = null;
        if ( location != null && location.getFileName() != null )
        {
            for ( JavaSourceFile file : sourceFiles )
            {
                if ( location.getFileName().endsWith( file.getName() ) )
                {
                    source = file;
                    break;
                }
            }
        }
        return source;
    }

    private static class CompiledByteCodes implements ByteCodes
    {
        private final String name;
        private final byte[] bytes;

        CompiledByteCodes( ClassFile classFile )
        {
            this.name = classFile.getThisClassName();
            this.bytes = classFile.toByteArray();
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public ByteBuffer bytes()
        {
            return ByteBuffer.wrap( bytes );
        }
    }

    private static class SourceReader extends Reader
    {
        private JavaSourceFile file;
        private int pos;

        SourceReader( JavaSourceFile file )
        {
            this.file = file;
        }

        @Override
        public int read( char[] cbuf, int off, int len ) throws IOException
        {
            if ( file == null )
            {
                throw new IOException( "Reader has been closed" );
            }
            if ( len <= 0 )
            {
                return 0;
            }
            int read = file.read( pos, cbuf, off, len );
            pos += read;
            if ( read <= 0 )
            {
                return -1; // EOF
            }
            return read;
        }

        @Override
        public void close() throws IOException
        {
            file = null;
        }
    }

    private static class Failure implements Diagnostic<JavaSourceFile>
    {
        private final Exception cause;
        private final JavaSourceFile file;
        private final Location location;

        public Failure( Exception cause, JavaSourceFile file, Location location )
        {
            this.cause = cause;
            this.file = file;
            this.location = location;
        }

        @Override
        public Kind getKind()
        {
            return Kind.ERROR;
        }

        @Override
        public JavaSourceFile getSource()
        {
            return file;
        }

        @Override
        public long getPosition()
        {
            return NOPOS;
        }

        @Override
        public long getStartPosition()
        {
            return NOPOS;
        }

        @Override
        public long getEndPosition()
        {
            return NOPOS;
        }

        @Override
        public long getLineNumber()
        {
            return location == null ? NOPOS : location.getLineNumber();
        }

        @Override
        public long getColumnNumber()
        {
            return location == null ? NOPOS : location.getColumnNumber();
        }

        @Override
        public String getCode()
        {
            return cause.getClass().getName();
        }

        @Override
        public String getMessage( Locale locale )
        {
            return cause.getMessage();
        }
    }
}
