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

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.codegen.source.SourceCode.SOURCECODE;

public abstract class CodeGenerationStrategy<Configuration> implements CodeGeneratorOption
{
    protected abstract Configuration createConfigurator( ClassLoader loader );

    protected abstract CodeGenerator createCodeGenerator( ClassLoader loader, Configuration configuration )
            throws CodeGenerationStrategyNotSupportedException;

    static CodeGenerator codeGenerator( ClassLoader loader, CodeGeneratorOption... options )
            throws CodeGenerationNotSupportedException
    {
        return applyTo( new Choice( SOURCECODE ), options ).generateCode( loader, options );
    }

    @Override
    public final void applyTo( Object target )
    {
        if ( target instanceof Choice )
        {
            ((Choice) target).setStrategy( this );
        }
    }

    private CodeGenerator generateCode( ClassLoader loader, CodeGeneratorOption... options )
            throws CodeGenerationStrategyNotSupportedException
    {
        Configuration configurator = createConfigurator( loader );
        return createCodeGenerator( loader, applyTo( configurator, options ) );
    }

    @Override
    public String toString()
    {
        return "CodeGenerationStrategy:" + name();
    }

    protected abstract String name();

    private static class Choice implements ByteCodeVisitor.Configurable
    {
        private CodeGenerationStrategy<?> strategy;
        private List<ByteCodeVisitor> visitors;

        private Choice( CodeGeneratorOption option )
        {
            option.applyTo( this );
        }

        void setStrategy( CodeGenerationStrategy<?> strategy )
        {
            this.strategy = strategy;
        }

        @Override
        public void addByteCodeVisitor( ByteCodeVisitor visitor )
        {
            if ( visitors == null )
            {
                visitors = new ArrayList<>();
            }
            visitors.add( visitor );
        }

        CodeGenerator generateCode( ClassLoader loader, CodeGeneratorOption[] options )
                throws CodeGenerationNotSupportedException
        {
            CodeGenerator generator = strategy.generateCode( loader, options );
            if ( visitors != null )
            {
                if ( visitors.size() == 1 )
                {
                    generator.setByteCodeVisitor( visitors.get( 0 ) );
                }
                else
                {
                    generator.setByteCodeVisitor( new ByteCodeVisitor.Multiplex(
                            visitors.toArray( new ByteCodeVisitor[visitors.size()] ) ) );
                }
            }
            return generator;
        }
    }

    private static <Target> Target applyTo( Target target, CodeGeneratorOption[] options )
    {
        if ( target instanceof Object[] )
        {
            for ( Object object : (Object[]) target )
            {
                applyTo( object, options );
            }
        }
        else
        {
            for ( CodeGeneratorOption option : options )
            {
                option.applyTo( target );
            }
        }
        return target;
    }
}
