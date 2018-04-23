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
package org.neo4j.causalclustering.helper;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ErrorHandlerTest
{
    private static final String FAILMESSAGE = "More fail";

    @Test
    public void shouldExecuteAllFailingOperations()
    {
        AtomicBoolean bool = new AtomicBoolean( false );
        try
        {
            ErrorHandler.runAll( "test", Assert::fail, () ->
            {
                bool.set( true );
                throw new IllegalStateException( FAILMESSAGE );
            } );
            fail();
        }
        catch ( RuntimeException e )
        {
            assertEquals( "test", e.getMessage() );
            Throwable cause = e.getCause();
            assertEquals( AssertionError.class, cause.getClass() );
            Throwable[] suppressed = e.getSuppressed();
            assertEquals( 1, suppressed.length );
            assertEquals( IllegalStateException.class, suppressed[0].getClass() );
            assertEquals( "More fail", suppressed[0].getMessage() );
            assertTrue( bool.get() );
        }
    }
}
