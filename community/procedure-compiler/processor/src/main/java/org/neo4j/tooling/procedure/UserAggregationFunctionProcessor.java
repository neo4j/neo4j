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

import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.tooling.procedure.compilerutils.CustomNameExtractor;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.visitors.FunctionVisitor;
import org.neo4j.tooling.procedure.visitors.UserAggregationFunctionVisitor;

import static org.neo4j.tooling.procedure.CompilerOptions.IGNORE_CONTEXT_WARNINGS_OPTION;

@AutoService( Processor.class )
public class UserAggregationFunctionProcessor extends DuplicationAwareBaseProcessor<UserAggregationFunction>
{

    public static final Class<UserAggregationFunction> SUPPORTED_ANNOTATION_TYPE = UserAggregationFunction.class;

    public UserAggregationFunctionProcessor()
    {
        super( SUPPORTED_ANNOTATION_TYPE, customNameExtractor(), processingEnvironment ->
        {
            Types typeUtils = processingEnvironment.getTypeUtils();
            Elements elementUtils = processingEnvironment.getElementUtils();
            TypeMirrorUtils typeMirrorUtils = new TypeMirrorUtils( typeUtils, elementUtils );

            return new UserAggregationFunctionVisitor(
                    new FunctionVisitor<>( SUPPORTED_ANNOTATION_TYPE, typeUtils, elementUtils, typeMirrorUtils,
                            customNameExtractor(),
                            processingEnvironment.getOptions().containsKey( IGNORE_CONTEXT_WARNINGS_OPTION ) ),
                    typeUtils );
        } );
    }

    private static Function<UserAggregationFunction,Optional<String>> customNameExtractor()
    {
        return function -> CustomNameExtractor.getName( function::name, function::value );
    }
}
