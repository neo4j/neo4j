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
package org.neo4j.codegen;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;

import static org.neo4j.codegen.ByteCodeVisitor.printer;

public abstract class DisassemblyVisitor implements ByteCodeVisitor, CodeGeneratorOption
{
    @Override
    public final void applyTo( Object target )
    {
        if ( target instanceof ByteCodeVisitor.Configurable )
        {
            ((Configurable) target).addByteCodeVisitor( this );
        }
    }

    @Override
    public final void visitByteCode( String name, ByteBuffer bytes )
    {
        StringWriter target = new StringWriter();
        try ( PrintWriter writer = new PrintWriter( target ) )
        {
            printer( writer ).visitByteCode( name, bytes );
        }
        visitDisassembly( name, target.getBuffer() );
    }

    protected abstract void visitDisassembly( String className, CharSequence disassembly );
}
