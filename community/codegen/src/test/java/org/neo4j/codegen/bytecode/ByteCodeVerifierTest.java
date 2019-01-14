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

import org.junit.Test;

import org.neo4j.codegen.ClassGenerator;
import org.neo4j.codegen.ClassHandle;
import org.neo4j.codegen.CodeBlock;
import org.neo4j.codegen.CodeGenerator;
import org.neo4j.codegen.CompilationFailureException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.codegen.CodeGenerationTest.PACKAGE;
import static org.neo4j.codegen.CodeGenerator.generateCode;
import static org.neo4j.codegen.Parameter.param;
import static org.neo4j.codegen.bytecode.ByteCode.BYTECODE;
import static org.neo4j.codegen.bytecode.ByteCode.VERIFY_GENERATED_BYTECODE;

public class ByteCodeVerifierTest
{
    @Test
    public void shouldVerifyBytecode() throws Throwable
    {
        // given
        CodeGenerator generator = generateCode( BYTECODE, VERIFY_GENERATED_BYTECODE );

        ClassHandle handle;
        try ( ClassGenerator clazz = generator.generateClass( PACKAGE, "SimpleClass" );
              CodeBlock code = clazz.generateMethod( Integer.class, "box", param( int.class, "value" ) ) )
        {
            handle = clazz.handle();
            code.returns( code.load( "value" ) );
        }

        // when
        try
        {
            handle.loadClass();
            fail( "Should have thrown exception" );
        }
        // then
        catch ( CompilationFailureException expected )
        {
            assertThat( expected.toString(), containsString( "box(I)" ) );
        }
    }
}
