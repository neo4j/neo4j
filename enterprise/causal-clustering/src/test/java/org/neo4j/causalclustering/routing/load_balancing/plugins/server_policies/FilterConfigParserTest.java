/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
