/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.shell.parameter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.cypher.internal.literal.interpreter.LiteralInterpreter;
import org.neo4j.cypher.internal.parser.javacc.Cypher;
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ParameterException;
import org.neo4j.shell.log.Logger;
import org.neo4j.values.storable.Value;

import static org.neo4j.shell.prettyprint.CypherVariablesFormatter.unescapedCypherVariable;

public interface ParameterService
{
    /**
     * Returns all set parameters.
     */
    Map<String, Parameter> parameters();

    /**
     * Returns all parameter values by name.
     */
    Map<String, Object> parameterValues();

    /**
     * Evaluate parameter. Simple expressions are evaluated offline, but complex needs an open connection.
     */
    Parameter evaluate( RawParameter parameter ) throws CommandException;

    /**
     * Set parameter.
     */
    void setParameter( Parameter parameter );

    /**
     * Returns parsed parameter.
     */
    RawParameter parse( String input ) throws ParameterParsingException;

    static ParameterService create( TransactionHandler db )
    {
        return new ShellParameterService( db );
    }

    static ParameterParser createParser()
    {
        return new ShellParameterService.ShellParameterParser();
    }

    interface ParameterParser
    {
        RawParameter parse( String input ) throws ParameterParsingException;
    }

    interface ParameterEvaluator
    {
        Parameter evaluate( RawParameter parameter ) throws CommandException;
    }

    record RawParameter( String name, String expression )
    { }

    record Parameter( String name, String expressionString, Object value )
    { }

    class ParameterParsingException extends Exception
    { }
}

class ShellParameterService implements ParameterService
{
    private static final Logger log = Logger.create();
    private final Map<String, Parameter> parameters = new HashMap<>( 0 );
    private final Map<String, Object> parameterValues = new HashMap<>( 0 );
    private final ParameterParser parser = new ShellParameterParser();
    private final ParameterEvaluator evaluator;

    ShellParameterService( TransactionHandler db )
    {
        this.evaluator = new ShellParameterEvaluator( db );
    }

    @Override
    public Map<String, Parameter> parameters()
    {
        return parameters;
    }

    @Override
    public Map<String, Object> parameterValues()
    {
        return parameterValues;
    }

    @Override
    public void setParameter( Parameter parameter )
    {
        parameters.put( parameter.name(), parameter );
        parameterValues.put( parameter.name(), parameter.value() );
    }

    @Override
    public Parameter evaluate( RawParameter parameter ) throws CommandException
    {
        return evaluator.evaluate( parameter );
    }

    @Override
    public RawParameter parse( String input ) throws ParameterParsingException
    {
        return parser.parse( input );
    }

    public static class ShellParameterParser implements ParameterParser
    {
        private final List<Pattern> patterns = List.of(
                Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*)\\s*=>\\s*(?<value>.+)$" ),
                Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):?\\s+(?<value>.+)$" ),
                Pattern.compile( "^\\s*(?<key>(`([^`])*`)+?)\\s*=>\\s*(?<value>.+)$" ),
                Pattern.compile( "^\\s*(?<key>(`([^`])*`)+?):?\\s+(?<value>.+)$" )
        );
        private final Pattern invalidPattern = Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):\\s*=>\\s*(?<value>.+)$" );

        @Override
        public RawParameter parse( String input ) throws ParameterParsingException
        {
            if ( invalidPattern.matcher( input ).matches() )
            {
                throw new ParameterParsingException();
            }

            return patterns.stream()
                           .map( p -> p.matcher( input ) )
                           .filter( Matcher::matches )
                           .findFirst()
                           .map( m -> new RawParameter( unescapedCypherVariable( m.group( "key" ) ), m.group( "value" ) ) )
                           .filter( p -> !p.name().isBlank() )
                           .orElseThrow( ParameterParsingException::new );
        }
    }

    private class ShellParameterEvaluator implements ParameterEvaluator
    {
        private final LiteralInterpreter interpreter = new LiteralInterpreter();
        private final TransactionHandler db;

        private ShellParameterEvaluator( TransactionHandler db )
        {
            this.db = db;
        }

        @Override
        public Parameter evaluate( RawParameter parameter ) throws CommandException
        {
            return evaluateOffline( parameter )
                    .or( () -> evaluateOnline( parameter ) )
                    .map( v -> new Parameter( parameter.name(), parameter.expression(), v ) )
                    .orElseThrow( () -> failedToEvaluate( parameter ) );
        }

        private CommandException failedToEvaluate( RawParameter parameter )
        {
            return new CommandException( "Failed to evaluate parameter " + parameter.name() + ": " + parameter.expression() );
        }

        @SuppressWarnings( { "rawtypes", "unchecked" } )
        private Optional<Object> evaluateOffline( RawParameter parameter )
        {
            Object value;
            try
            {
                value = new Cypher( interpreter, ParameterException.FACTORY, new CypherCharStream( parameter.expression() ) ).Expression();
            }
            catch ( Exception e )
            {
                log.warn( "Failed to evaluate expression " + parameter.expression() + " locally", e );
                return Optional.empty();
            }

            if ( value instanceof Value neo4jValue )
            {
                value = neo4jValue.asObject();
            }

            return Optional.ofNullable( value );
        }

        private Optional<Object> evaluateOnline( RawParameter parameter )
        {
            try
            {
                // Feels very wrong to execute user data unescaped...
                var query = "RETURN " + parameter.expression() + " AS `result`;";

                return db.runCypher( query, parameterValues() ).map( r -> r.iterate().next().get( "result" ).asObject() );
            }
            catch ( Exception e )
            {
                log.error( "Failed to evaluate expression " + parameter.expression() + " online", e );
                return Optional.empty();
            }
        }
    }
}
