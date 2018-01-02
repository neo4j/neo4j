/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class VersionTest
{
    @Test
    public void shouldExposeCleanAndDetailedVersions() throws Throwable
    {
        assertThat( version( "1.2.3-M01,abcdef012345" ).getReleaseVersion(), equalTo( "1.2.3-M01" ));
        assertThat( version( "1.2.3-M01,abcdef012345" ).getVersion(),        equalTo( "1.2.3-M01,abcdef012345" ));
        assertThat( version( "1.2.3-M01,abcdef012345-dirty" ).getVersion(),  equalTo( "1.2.3-M01,abcdef012345-dirty" ));

        assertThat( version( "1.2.3,abcdef012345" ).getReleaseVersion(),     equalTo( "1.2.3" ));
        assertThat( version( "1.2.3,abcdef012345" ).getVersion(),            equalTo( "1.2.3,abcdef012345" ));
        assertThat( version( "1.2.3,abcdef012345-dirty" ).getVersion(),      equalTo( "1.2.3,abcdef012345-dirty" ));

        assertThat( version( "1.2.3-GA,abcdef012345" ).getReleaseVersion(),  equalTo( "1.2.3-GA" ));
        assertThat( version( "1.2.3-GA,abcdef012345" ).getVersion(),         equalTo( "1.2.3-GA,abcdef012345" ));
        assertThat( version( "1.2.3-GA,abcdef012345-dirty" ).getVersion(),   equalTo( "1.2.3-GA,abcdef012345-dirty" ));

        assertThat( version( "1.2.3M01,abcdef012345" ).getReleaseVersion(),  equalTo( "1.2.3M01" ));
        assertThat( version( "1.2.3M01,abcdef012345" ).getVersion(),         equalTo( "1.2.3M01,abcdef012345" ));
        assertThat( version( "1.2.3M01,abcdef012345-dirty" ).getVersion(),   equalTo( "1.2.3M01,abcdef012345-dirty" ));

        assertThat( version( "1.2" ).getReleaseVersion(),  equalTo( "1.2" ));
        assertThat( version( "1.2" ).getVersion(),         equalTo( "1.2" ));

        assertThat( version( "0" ).getReleaseVersion(),    equalTo( "0" ));
        assertThat( version( "0" ).getVersion(),           equalTo( "0" ));
    }

    private Version version( String version )
    {
        return new Version( "test-component", version );
    }
}