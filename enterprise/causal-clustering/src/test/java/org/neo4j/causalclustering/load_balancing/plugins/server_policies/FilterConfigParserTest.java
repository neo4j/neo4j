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
package org.neo4j.causalclustering.load_balancing.plugins.server_policies;

import org.junit.Test;

import org.neo4j.causalclustering.load_balancing.filters.Filter;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.load_balancing.plugins.server_policies.FilterBuilder.filter;

public class FilterConfigParserTest
{
    @Test
    public void shouldThrowExceptionOnInvalidConfig() throws Exception
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
                "tags",
                "min",
                "unknown",
                "unknown()",
                "unknown(something)",
                "min()",
                "min(2,5)",
                "tags()",
                "all(2)",
                "min(five)",
                "tags(tag1_%)",
                "tags(tag2_%)",
                "tags(tag 2)",
                "%tags(tag2)",
                "ta%gs(tag2)",
                "ta-gs(tag2)",
                "tags(tag1),tags(tag2)",
                "tags(tag1);;tags(tag2)",
                "tags(tag1)+tags(tag2)",
                "halt();tags(tag)",
                "halt();halt()",
                "tags(tag1);halt();tags(tag2)",
                "tags(tag1);tags(tag2);halt();tags(tag3)",
                "tags(tag1) -> halt()",
                "halt() -> tags(tag1)",
                "tags(tag1) -> tags(tag2) -> halt()",
                "tags(tag1) -> halt() -> tags(tag2)",
                "tags(tag)->all()",
                "all()->all()",
                "tags(A)->all()->tags(B)",
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
    public void shouldParseValidConfigs() throws Exception
    {
        Object[][] validConfigs = {
                {
                        "min(2);",
                        filter().min( 2 )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "tags(5);",
                        filter().tags( "5" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "all()",
                        filter().all().build()
                },
                {
                        "all() -> tags(5);",
                        filter().tags( "5" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "all() -> tags(5);all()",
                        filter().tags( "5" )
                                .newRule().all()
                                .build()
                },
                {
                        "all() -> tags(A); all() -> tags(B); halt()",
                        filter().tags( "A" )
                                .newRule().tags( "B" )
                                .build()
                },
                {
                        "tags(tagA);",
                        filter().tags( "tagA" )
                                .newRule().all() // implicit
                                .build()
                },
                {
                        "tags(tagA,tagB); halt()",
                        filter().tags( "tagA", "tagB" ).build()
                },
                {
                        "tags ( tagA , tagB ); halt()",
                        filter().tags( "tagA", "tagB" ).build()
                },
                {
                        "tags(tag1)->tags(tag2); halt()",
                        filter().tags( "tag1" ).tags( "tag2" ).build()
                },
                {
                        "tags(tag1)->tags(tag2); halt();",
                        filter().tags( "tag1" ).tags( "tag2" ).build()
                },
                {
                        "tags(tag1)->tags(tag2)->min(4); tags(tag3,tag4)->min(2); halt();",
                        filter().tags( "tag1" ).tags( "tag2" ).min( 4 )
                                .newRule().tags( "tag3", "tag4" ).min( 2 ).build()
                },
                {
                        "tags(tag1,tag2,tag3,tag4)->min(2); tags(tag3,tag4);",
                        filter().tags( "tag1", "tag2", "tag3", "tag4" ).min( 2 )
                                .newRule().tags( "tag3", "tag4" )
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
