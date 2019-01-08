/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.fs;

import java.io.IOException;
import java.nio.file.FileVisitResult;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class JustContinueTest
{
    @Test
    public void shouldJustContinue() throws IOException
    {
        assertThat( FileVisitors.justContinue().preVisitDirectory( null, null ), is( FileVisitResult.CONTINUE ) );
        assertThat( FileVisitors.justContinue().visitFile( null, null ), is( FileVisitResult.CONTINUE ) );
        assertThat( FileVisitors.justContinue().visitFileFailed( null, null ), is( FileVisitResult.CONTINUE ) );
        assertThat( FileVisitors.justContinue().postVisitDirectory( null, null ), is( FileVisitResult.CONTINUE ) );
    }
}
