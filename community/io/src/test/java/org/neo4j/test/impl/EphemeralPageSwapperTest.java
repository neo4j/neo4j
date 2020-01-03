/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.test.impl;

import java.io.File;

import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PageSwapperTest;

public class EphemeralPageSwapperTest extends PageSwapperTest
{
    @Override
    protected PageSwapperFactory swapperFactory()
    {
        return new EphemeralPageSwapperFactory();
    }

    @Override
    protected void mkdirs( File dir )
    {
    }

    @Override
    protected void positionedVectoredReadWhereLastPageExtendBeyondEndOfFileMustHaveRemainderZeroFilled()
    {
        // Disable this test because it maps the same file with different buffer sizes, and the ephemeral swapper does not support that.
    }

    @Override
    protected void positionedVectoredReadWhereSecondLastPageExtendBeyondEndOfFileMustHaveRestZeroFilled() throws Exception
    {
        // Disable this test because it maps the same file with different buffer sizes, and the ephemeral swapper does not support that.
    }
}
