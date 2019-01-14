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

import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.PerformsWriteMisuseError;

import java.util.stream.Stream;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.util.SimpleElementVisitor8;

import org.neo4j.procedure.Mode;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

public class PerformsWriteMethodVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{

    @Override
    public Stream<CompilationMessage> visitExecutable( ExecutableElement method, Void ignored )
    {
        Procedure procedure = method.getAnnotation( Procedure.class );
        if ( procedure == null )
        {
            return Stream.of( new PerformsWriteMisuseError( method, "@%s usage error: missing @%s annotation on method",
                    PerformsWrites.class.getSimpleName(), Procedure.class.getSimpleName() ) );
        }

        if ( procedure.mode() != Mode.DEFAULT )
        {
            return Stream.of( new PerformsWriteMisuseError( method,
                    "@%s usage error: cannot use mode other than Mode.DEFAULT",
                    PerformsWrites.class.getSimpleName() ) );
        }
        return Stream.empty();
    }

}
