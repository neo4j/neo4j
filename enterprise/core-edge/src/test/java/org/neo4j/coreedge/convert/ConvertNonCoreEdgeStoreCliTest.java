/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.convert;

import org.junit.Test;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

public class ConvertNonCoreEdgeStoreCliTest
{

    @Test
    public void shouldIndicateMissingHomeDir() throws Throwable
    {
        try
        {
            // given
            ConvertNonCoreEdgeStoreCli.main( new String[]{""} );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException exception )
        {
            assertTrue(exception.getMessage(), exception.getMessage().contains( "Missing argument 'home-dir'" ) );
        }
    }

    @Test
    public void shouldIndicateMissingDatabase() throws Throwable
    {
        try
        {
            // given
            ConvertNonCoreEdgeStoreCli.main( new String[]{"--home-dir", "foo"} );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException exception )
        {
            assertTrue(exception.getMessage(), exception.getMessage().contains( "Missing argument 'database'" ) );
        }
    }

    @Test
    public void shouldIndicateMissingConfig() throws Throwable
    {
        try
        {
            // given
            ConvertNonCoreEdgeStoreCli.main( new String[]{"--home-dir", "foo", "--database", "foo"} );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException exception )
        {
            assertTrue(exception.getMessage(), exception.getMessage().contains( "Missing argument 'config'" ) );
        }
    }

}
