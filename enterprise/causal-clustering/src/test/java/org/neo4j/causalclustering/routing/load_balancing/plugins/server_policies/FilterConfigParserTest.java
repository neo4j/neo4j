/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import org.junit.Test;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.FilterBuilder.filter;

public class FilterConfigParserTest
{
    @Test
    public void shouldThrowExceptionOnInvalidConfig()
    {
        String[] invalidConfigs = {
                "",
                ";",
                "(",
                ")",
                "()",
                ",",
                "\"",
                "\'",
                "groups",
                "min",
                "unknown",
                "unknown()",
                "unknown(something)",
                "min()",
                "min(2,5)",
                "groups()",
                "all(2)",
                "min(five)",
                "groups(group1_%)",
                "groups(group2_%)",
                "groups(group 2)",
                "%groups(group2)",
                "ta%gs(group2)",
                "ta-gs(group2)",
                "groups(group1),groups(group2)",
                "groups(group1);;groups(group2)",
                "groups(group1)+groups(group2)",
                "halt();groups(group)",
                "halt();halt()",
                "groups(group1);halt();groups(group2)",
                "groups(group1);groups(group2);halt();groups(group3)",
                "groups(group1) -> halt()",
                "halt() -> groups(group1)",
                "groups(group1) -> groups(group2) -> halt()",
                "groups(group1) -> halt() -> groups(group2)",
                "groups(group)->all()",
                "all()->all()",
                "groups(A)->all()->groups(B)",
        };

        // when
        for ( String invalidConfig : invalidConfigs )
        {
            try
            {
                Filter<ServerInfo> parsedFilter = FilterConfigParser.parse( invalidConfig );
                fail( format( "Config should have been invalid: '%s' but generated: %s", invalidConfig, parsedFilter ) );
            }
            catch ( InvalidFilterSpecification e )
            {
                // expected
            }
        }
    }

    @Test
    public void shouldParseValidConfigs()
    {
        Object[][] validConfigs = {
                {
                        "min(2);",
                        filter().min( 2 )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "groups(5);",
                        filter().groups( "5" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "all()",
                        filter().all().build()
                },
                {
                        "all() -> groups(5);",
                        filter().groups( "5" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "all() -> groups(5);all()",
                        filter().groups( "5" )
                                .newRule().all()
                                .build()
                },
                {
                        "all() -> groups(A); all() -> groups(B); halt()",
                        filter().groups( "A" )
                                .newRule().groups( "B" )
                                .build()
                },
                {
                        "groups(groupA);",
                        filter().groups( "groupA" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "groups(groupA,groupB); halt()",
                        filter().groups( "groupA", "groupB" ).build()
                },
                {
                        "groups ( groupA , groupB ); halt()",
                        filter().groups( "groupA", "groupB" ).build()
                },
                {
                        "groups(group1)->groups(group2); halt()",
                        filter().groups( "group1" ).groups( "group2" ).build()
                },
                {
                        "groups(group1)->groups(group2); halt();",
                        filter().groups( "group1" ).groups( "group2" ).build()
                },
                {
                        "groups(group-1)->groups(group-2); halt();",
                        filter().groups( "group-1" ).groups( "group-2" ).build()
                },
                {
                        "groups(group1)->groups(group2)->min(4); groups(group3,group4)->min(2); halt();",
                        filter().groups( "group1" ).groups( "group2" ).min( 4 )
                                .newRule().groups( "group3", "group4" ).min( 2 ).build()
                },
                {
                        "groups(group1,group2,group3,group4)->min(2); groups(group3,group4);",
                        filter().groups( "group1", "group2", "group3", "group4" ).min( 2 )
                                .newRule().groups( "group3", "group4" )
                                .newRule().all() // implicit
                                .build()
                }
        };

        // when
        for ( Object[] validConfig : validConfigs )
        {
            String config = (String) validConfig[0];
            Filter expectedFilter = (Filter) validConfig[1];

            try
            {
                Filter<ServerInfo> parsedFilter = FilterConfigParser.parse( config );
                assertEquals( format( "Config '%s' should generate expected filter", config ),
                        expectedFilter, parsedFilter );
            }
            catch ( InvalidFilterSpecification e )
            {
                fail( format( "Config should have been valid: '%s'", config ) );
            }
        }
    }
}
