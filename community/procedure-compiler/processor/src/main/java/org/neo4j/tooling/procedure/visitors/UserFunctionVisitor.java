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
package org.neo4j.tooling.procedure.visitors;

import java.util.function.Function;
import java.util.stream.Stream;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.util.SimpleElementVisitor8;

import org.neo4j.procedure.UserFunction;
import org.neo4j.tooling.procedure.messages.CompilationMessage;

public class UserFunctionVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{

    private final FunctionVisitor functionVisitor;

    public UserFunctionVisitor( FunctionVisitor<UserFunction> baseFunctionVisitor )
    {
        this.functionVisitor = baseFunctionVisitor;
    }

    @Override
    public Stream<CompilationMessage> visitExecutable( ExecutableElement executableElement, Void ignored )
    {
        return Stream.<Stream<CompilationMessage>>of( functionVisitor.validateEnclosingClass( executableElement ),
                functionVisitor.validateParameters( executableElement.getParameters() ),
                functionVisitor.validateName( executableElement ),
                functionVisitor.validateReturnType( executableElement ) ).flatMap( Function.identity() );
    }

}
