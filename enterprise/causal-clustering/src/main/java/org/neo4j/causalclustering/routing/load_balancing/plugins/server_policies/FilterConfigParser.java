/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import org.neo4j.causalclustering.routing.load_balancing.filters.FilterChain;
import org.neo4j.causalclustering.routing.load_balancing.filters.FirstValidRule;
import org.neo4j.causalclustering.routing.load_balancing.filters.IdentityFilter;
import org.neo4j.causalclustering.routing.load_balancing.filters.MinimumCountFilter;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class FilterConfigParser
{
    private FilterConfigParser()
    {
    }

    private static Filter<ServerInfo> filterFor( String filterName, String[] args ) throws InvalidFilterSpecification
    {
        switch ( filterName )
        {
        case "groups":
            if ( args.length < 1 )
            {
                throw new InvalidFilterSpecification(
                        format( "Invalid number of arguments for filter '%s': %d", filterName, args.length ) );
            }
            for ( String group : args )
            {
                if ( group.matches( "\\W" ) )
                {
                    throw new InvalidFilterSpecification(
                            format( "Invalid group for filter '%s': '%s'", filterName, group ) );
                }
            }
            return new AnyGroupFilter( args );
        case "min":
            if ( args.length != 1 )
            {
                throw new InvalidFilterSpecification(
                        format( "Invalid number of arguments for filter '%s': %d", filterName, args.length ) );
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
                throw new InvalidFilterSpecification(
                        format( "Invalid number of arguments for filter '%s': %d", filterName, args.length ) );
            }
            return IdentityFilter.as();
        case "halt":
            if ( args.length != 0 )
            {
                throw new InvalidFilterSpecification(
                        format( "Invalid number of arguments for filter '%s': %d", filterName, args.length ) );
            }
            return HaltFilter.INSTANCE;
        default:
            throw new InvalidFilterSpecification( "Unknown filter: " + filterName );
        }
    }

    public static Filter<ServerInfo> parse( String filterConfig ) throws InvalidFilterSpecification
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

        boolean haltFilterEncountered = false;
        for ( String ruleSpec : ruleSpecs )
        {
            ruleSpec = ruleSpec.trim();

            List<Filter<ServerInfo>> filterChain = new ArrayList<>();
            String[] filterSpecs = ruleSpec.split( "->" );
            boolean allFilterEncountered = false;
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
                    if ( !arg.matches( "[\\w-]+" ) )
                    {
                        throw new InvalidFilterSpecification( format( "Syntax error argument: '%s'", arg ) );
                    }
                }

                if ( haltFilterEncountered )
                {
                    if ( filterChain.size() > 0 )
                    {
                        throw new InvalidFilterSpecification(
                                format( "Filter 'halt' may not be followed by other filters: '%s'", ruleSpec ) );
                    }
                    else
                    {
                        throw new InvalidFilterSpecification(
                                format( "Rule 'halt' may not followed by other rules: '%s'", filterConfig ) );
                    }
                }

                Filter<ServerInfo> filter = filterFor( filterName, nonEmptyArgs );

                if ( filter == HaltFilter.INSTANCE )
                {
                    if ( filterChain.size() != 0 )
                    {
                        throw new InvalidFilterSpecification(
                                format( "Filter 'halt' must be the only filter in a rule: '%s'", ruleSpec ) );
                    }
                    haltFilterEncountered = true;
                }
                else if ( filter == IdentityFilter.INSTANCE )
                {
                    /* The all() filter is implicit and unnecessary, but still allowed in the beginning of a rule for clarity
                     * and for allowing the actual rule consisting of only all() to be specified. */

                    if ( allFilterEncountered || filterChain.size() != 0 )
                    {
                        throw new InvalidFilterSpecification(
                                format( "Filter 'all' is implicit but allowed only first in a rule: '%s'", ruleSpec ) );
                    }

                    allFilterEncountered = true;
                }
                else
                {
                    filterChain.add( filter );
                }
            }

            if ( filterChain.size() > 0 )
            {
                rules.add( new FilterChain<>( filterChain ) );
            }
        }

        if ( !haltFilterEncountered )
        {
            /* we implicitly add the all() rule to the end if there was no explicit halt() */
            rules.add( new FilterChain<>( singletonList( IdentityFilter.as() ) ) );
        }

        return new FirstValidRule<>( rules );
    }
}
