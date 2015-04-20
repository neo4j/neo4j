/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommunityBootstrapperTest extends BaseBootstrapperTest
{
    @Override
    protected Class<? extends Bootstrapper> bootstrapperClass()
    {
        return CommunityBootstrapper.class;
    }

    @Override
    protected Bootstrapper newBootstrapper()
    {
        return new CommunityBootstrapper();
    }

    @Test
    public void shouldFindTheMostDerivedType() throws Exception
    {
        Bootstrapper bs = new CommunityBootstrapper();
        Bootstrapper other = new MoreDerivedBootstrapper();
        assertFalse( bs.isMoreDerivedThan( other ) );
        assertTrue( other.isMoreDerivedThan( bs ) );
    }

    private static class MoreDerivedBootstrapper extends CommunityBootstrapper
    {
    }
}
