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
package org.neo4j.tooling.procedure;

import com.google.auto.service.AutoService;

import java.util.Optional;
import java.util.function.Function;
import javax.annotation.processing.Processor;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import org.neo4j.procedure.UserFunction;
import org.neo4j.tooling.procedure.compilerutils.CustomNameExtractor;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.visitors.FunctionVisitor;
import org.neo4j.tooling.procedure.visitors.UserFunctionVisitor;

import static org.neo4j.tooling.procedure.CompilerOptions.IGNORE_CONTEXT_WARNINGS_OPTION;

@AutoService( Processor.class )
public class UserFunctionProcessor extends DuplicationAwareBaseProcessor<UserFunction>
{

    private static final Class<UserFunction> SUPPORTED_ANNOTATION_TYPE = UserFunction.class;

    public UserFunctionProcessor()
    {
        super( SUPPORTED_ANNOTATION_TYPE, customNameExtractor(), processingEnvironment ->
        {
            Elements elementUtils = processingEnvironment.getElementUtils();
            Types typeUtils = processingEnvironment.getTypeUtils();
            TypeMirrorUtils typeMirrorUtils = new TypeMirrorUtils( typeUtils, elementUtils );

            return new UserFunctionVisitor(
                    new FunctionVisitor<>( SUPPORTED_ANNOTATION_TYPE, typeUtils, elementUtils, typeMirrorUtils,
                            customNameExtractor(),
                            processingEnvironment.getOptions().containsKey( IGNORE_CONTEXT_WARNINGS_OPTION ) ) );
        } );
    }

    private static Function<UserFunction,Optional<String>> customNameExtractor()
    {
        return function -> CustomNameExtractor.getName( function::name, function::value );
    }
}
