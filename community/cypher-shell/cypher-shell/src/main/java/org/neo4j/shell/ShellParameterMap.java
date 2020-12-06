/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.cypher.internal.ast.factory.LiteralInterpreter;
import org.neo4j.cypher.internal.evaluator.EvaluationException;
import org.neo4j.cypher.internal.evaluator.Evaluator;
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator;
import org.neo4j.cypher.internal.parser.javacc.Cypher;
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream;
import org.neo4j.cypher.internal.parser.javacc.ParseException;
import org.neo4j.shell.exception.ParameterException;
import org.neo4j.shell.prettyprint.CypherVariablesFormatter;
import org.neo4j.shell.state.ParamValue;

/**
 * An object which keeps named parameters and allows them them to be set/unset.
 */
public class ShellParameterMap implements ParameterMap
{
    private final Map<String, ParamValue> queryParams = new HashMap<>();
    private LiteralInterpreter interpreter = new LiteralInterpreter();
    private ExpressionEvaluator evaluator = Evaluator.expressionEvaluator();

    @Override
    public Object setParameter( @Nonnull String name, @Nonnull String valueString ) throws ParameterException
    {
        String parameterName = CypherVariablesFormatter.unescapedCypherVariable( name );
        try
        {
            Object value = new Cypher( interpreter,
                                         ParameterException.FACTORY,
                                         new CypherCharStream( valueString ) ).Expression();
            queryParams.put( parameterName, new ParamValue( valueString, value ) );
            return value;
        }
        catch ( ParseException | UnsupportedOperationException e )
        {
            try
            {
                Object value = evaluator.evaluate( valueString, Object.class );
                queryParams.put( parameterName, new ParamValue( valueString, value ) );
                return value;
            }
            catch ( EvaluationException e1 )
            {
                throw new ParameterException( e1.getMessage() );
            }
        }
    }

    @Nonnull
    @Override
    public Map<String, Object> allParameterValues()
    {
        return queryParams.entrySet()
                          .stream()
                          .collect( Collectors.toMap(
                                  Map.Entry::getKey,
                                  value -> value.getValue().getValue() ) );
    }

    @Nonnull
    @Override
    public Map<String, ParamValue> getAllAsUserInput()
    {
        return queryParams;
    }
}
