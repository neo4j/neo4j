/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.webadmin.webtest;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith( ThirdTimeIsTheCharmTestRunner.class )
@Ignore
public class ThirdTimeIsTheCharmTest
{
    private static boolean alreadyFailed = false;

    @Test
    public void failOnce()
    {
        if ( !alreadyFailed )
        {
            alreadyFailed = true;
            fail();
        }
    }

    private static int failTwice = 0;
    @Test
    public void failTwice() throws Exception
    {
        if ( failTwice < 2 )
        {
            failTwice++;
            fail();
        }
    }

    private static int failThrice = 0;
    @Test
    public void failThrice() throws Exception
    {
        if ( failThrice < 3 )
        {
            failThrice++;
            fail();
        }
    }
}
