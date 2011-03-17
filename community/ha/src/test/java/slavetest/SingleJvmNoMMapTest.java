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
package slavetest;

import java.util.Map;

import static org.junit.Assume.assumeTrue;

import org.junit.BeforeClass;
import org.neo4j.kernel.Config;

/**
 * Test class meant to run only on Linux, checking that HA setups work even with
 * no memory mapped buffers.
 */
public class SingleJvmNoMMapTest extends SingleJvmTest
{
    @Override
    protected void addDb( Map<String, String> config )
    {
        config.put( Config.USE_MEMORY_MAPPED_BUFFERS, "false" );
        super.addDb( config );
    }

    @BeforeClass
    public static void onlyOnLinux()
    {
        assumeTrue( !Config.osIsWindows() );
    }
}
