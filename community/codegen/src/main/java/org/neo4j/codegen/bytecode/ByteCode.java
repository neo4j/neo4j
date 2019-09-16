/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.codegen.bytecode;

import org.neo4j.codegen.CodeGenerationStrategy;
import org.neo4j.codegen.CodeGenerator;
import org.neo4j.codegen.CodeGeneratorOption;
import org.neo4j.codegen.DisassemblyVisitor;

public enum ByteCode implements CodeGeneratorOption
{
    ;
    public static final CodeGenerationStrategy<?> BYTECODE = new CodeGenerationStrategy<Configuration>()
    {
        @Override
        protected Configuration createConfigurator( ClassLoader loader )
        {
            return new Configuration();
        }

        @Override
        protected CodeGenerator createCodeGenerator( ClassLoader loader, Configuration configuration )
        {
            return new ByteCodeGenerator( loader, configuration );
        }

        @Override
        protected String name()
        {
            return "BYTECODE";
        }
    };
    public static final CodeGeneratorOption VERIFY_GENERATED_BYTECODE = load( "Verifier" );
    public static final CodeGeneratorOption PRINT_BYTECODE = new DisassemblyVisitor()
    {
        @Override
        protected void visitDisassembly( String className, CharSequence disassembly )
        {
            String[] lines = disassembly.toString().split( "\\n" );
            System.out.println( "=== Generated class bytecode " + className + " ===\n" );
            for ( int i = 0; i < lines.length; i++ )
            {
                System.out.print( i + 1 );
                System.out.print( '\t' );
                System.out.println( lines[i] );
            }
        }

        @Override
        public String toString()
        {
            return "PRINT_BYTECODE";
        }
    };

    @Override
    public void applyTo( Object target )
    {
        if ( target instanceof Configuration )
        {
            ((Configuration) target).withFlag( this );
        }
    }

    private static CodeGeneratorOption load( String option )
    {
        try
        {
            return (CodeGeneratorOption) Class.forName( ByteCode.class.getName() + option )
                                              .getDeclaredMethod( "load" + option ).invoke( null );
        }
        catch ( Throwable e )
        {
            return BLANK_OPTION;
        }
    }
}
