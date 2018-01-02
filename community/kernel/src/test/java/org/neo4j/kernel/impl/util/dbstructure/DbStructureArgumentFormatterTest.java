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
package org.neo4j.kernel.impl.util.dbstructure;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

import static org.junit.Assert.assertEquals;

public class DbStructureArgumentFormatterTest
{
    @Test
    public void shouldFormatNull()
    {
        assertEquals( "null", formatArgument( null ) );
    }

    @Test
    public void shouldFormatInts()
    {
        assertEquals( "0", formatArgument( 0 ) );
        assertEquals( "1", formatArgument( 1 ) );
        assertEquals( "-1", formatArgument( -1 ) );
    }

    @Test
    public void shouldFormatLongs()
    {
        assertEquals( "0l", formatArgument( 0l ) );
        assertEquals( "-1l", formatArgument( -1l ) );
        assertEquals( "1l", formatArgument( 1l ) );
    }

    @Test
    public void shouldFormatDoubles()
    {
        assertEquals( "1.0d", formatArgument( 1.0d ) );
        assertEquals( "Double.NaN", formatArgument( Double.NaN ) );
        assertEquals( "Double.POSITIVE_INFINITY", formatArgument( Double.POSITIVE_INFINITY ) );
        assertEquals( "Double.NEGATIVE_INFINITY", formatArgument( Double.NEGATIVE_INFINITY ) );
    }

    @Test
    public void shouldFormatIndexDescriptors()
    {
        assertEquals( "new IndexDescriptor( 23, 42 )", formatArgument( new IndexDescriptor( 23, 42 ) ) );
    }

    @Test
    public void shouldFormatUniquenessConstraints()
    {
        assertEquals( "new UniquenessConstraint( 23, 42 )", formatArgument( new UniquenessConstraint( 23, 42 ) ) );
    }

    private String formatArgument( Object arg )
    {
        StringBuilder builder = new StringBuilder();
        try
        {
            DbStructureArgumentFormatter.INSTANCE.formatArgument( builder, arg );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return builder.toString();
    }
}
