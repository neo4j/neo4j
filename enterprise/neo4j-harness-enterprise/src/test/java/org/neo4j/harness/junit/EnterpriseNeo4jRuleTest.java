/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.harness.junit;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class EnterpriseNeo4jRuleTest
{
    @Rule
    public Neo4jRule neo4j = new Neo4jRule().withExtension( "/test", MyUnmanagedExtension.class );

    @Rule public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldExtensionWork() throws Exception
    {
        // Given the rule in the beginning of this class

        // When I run this test

        // Then
        assertThat( HTTP.GET( neo4j.httpURI().resolve( "test/myExtension" ).toString() ).status(), equalTo( 234 ) );
    }
}
