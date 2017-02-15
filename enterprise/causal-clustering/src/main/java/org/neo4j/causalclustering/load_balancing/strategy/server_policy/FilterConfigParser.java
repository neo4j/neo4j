/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.load_balancing.strategy.server_policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.load_balancing.filters.Filter;
import org.neo4j.causalclustering.load_balancing.filters.FilterChain;
import org.neo4j.causalclustering.load_balancing.filters.FirstValidRule;
import org.neo4j.causalclustering.load_balancing.filters.IdentityFilter;
import org.neo4j.causalclustering.load_balancing.filters.MinimumCountFilter;

import static java.lang.String.format;

class FilterConfigParser
{
    private static Filter<ServerInfo> filterFor( String filterName, String[] args ) throws InvalidFilterSpecification
    {
        switch ( filterName )
        {
        case "tags":
            if ( args.length < 1 )
            {
                throw new InvalidFilterSpecification( format( "Invalid number of arguments for filter '%s': %d", filterName, args.length ) );
            }
            for ( String tag : args )
            {
                if ( tag.matches( "\\W" ) )
                {
                    throw new InvalidFilterSpecification( format( "Invalid tag for filter '%s': '%s'", filterName, tag ) );
                }
            }
            return new AnyTagFilter( args );
        case "min":
            if ( args.length != 1 )
            {
                throw new InvalidFilterSpecification( format( "Invalid number of arguments for filter '%s': %d", filterName, args.length ) );
            }
            int minCount;
            try
            {
                minCount = Integer.parseInt( args[0] );
            }
            catch ( NumberFormatException e )
            {
                throw new InvalidFilterSpecification( format( "Invalid argument for filter '%s': '%s'", filterName, args[0] ), e );
            }
            return new MinimumCountFilter<>( minCount );
        case "all":
            if ( args.length != 0 )
            {
                throw new InvalidFilterSpecification( format( "Invalid number of arguments for filter '%s': %d", filterName, args.length ) );
            }
            return IdentityFilter.as();
        default:
            throw new InvalidFilterSpecification( "Unknown filter: " + filterName );
        }
    }

    static Filter<ServerInfo> parse( String filterConfig ) throws InvalidFilterSpecification
    {
        if ( filterConfig.length() == 0 )
        {
            throw new InvalidFilterSpecification( "Filter config is empty" );
        }

        List<FilterChain<ServerInfo>> rules = new ArrayList<>();
        String[] ruleSpecs = filterConfig.split( ";" );

        if ( ruleSpecs.length == 0 )
        {
            throw new InvalidFilterSpecification( "No rules specified" );
        }

        for ( String ruleSpec : ruleSpecs )
        {
            ruleSpec = ruleSpec.trim();

            List<Filter<ServerInfo>> filterChain = new ArrayList<>();
            String[] filterSpecs = ruleSpec.split( "->" );
            for ( String filterSpec : filterSpecs )
            {
                filterSpec = filterSpec.trim();

                String namePart;
                String argsPart;
                {
                    String[] nameAndArgs = filterSpec.split( "\\(", 0 );

                    if ( nameAndArgs.length != 2 )
                    {
                        throw new InvalidFilterSpecification( format( "Syntax error filter specification: '%s'", filterSpec ) );
                    }

                    namePart = nameAndArgs[0].trim();
                    argsPart = nameAndArgs[1].trim();
                }

                if ( !argsPart.endsWith( ")" ) )
                {
                    throw new InvalidFilterSpecification( format( "No closing parenthesis: '%s'", filterSpec ) );
                }
                argsPart = argsPart.substring( 0, argsPart.length() - 1 );

                String filterName = namePart.trim();
                if ( !filterName.matches( "\\w+" ) )
                {
                    throw new InvalidFilterSpecification( format( "Syntax error filter name: '%s'", filterName ) );
                }

                String[] nonEmptyArgs = Arrays.stream(
                        argsPart.split( "," ) )
                        .map( String::trim )
                        .filter( s -> !s.isEmpty() )
                        .collect( Collectors.toList() )
                        .toArray( new String[0] );

                for ( String arg : nonEmptyArgs )
                {
                    if ( !arg.matches( "\\w+" ) )
                    {
                        throw new InvalidFilterSpecification( format( "Syntax error argument: '%s'", arg ) );
                    }
                }

                Filter<ServerInfo> filter = filterFor( filterName, nonEmptyArgs );
                filterChain.add( filter );
            }

            rules.add( new FilterChain<>( filterChain ) );
        }

        return new FirstValidRule<>( rules );
    }
}
