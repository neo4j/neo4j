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
package org.neo4j.kernel.impl.util.dbstructure;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DbStructureArgumentFormatterTest
{
    @Test
    void shouldFormatNull()
    {
        assertEquals( "null", formatArgument( null ) );
    }

    @Test
    void shouldFormatInts()
    {
        assertEquals( "0", formatArgument( 0 ) );
        assertEquals( "1", formatArgument( 1 ) );
        assertEquals( "-1", formatArgument( -1 ) );
    }

    @Test
    void shouldFormatLongs()
    {
        assertEquals( "0L", formatArgument( 0L ) );
        assertEquals( "-1L", formatArgument( -1L ) );
        assertEquals( "1L", formatArgument( 1L ) );
    }

    @Test
    void shouldFormatDoubles()
    {
        assertEquals( "1.0d", formatArgument( 1.0d ) );
        assertEquals( "Double.NaN", formatArgument( Double.NaN ) );
        assertEquals( "Double.POSITIVE_INFINITY", formatArgument( Double.POSITIVE_INFINITY ) );
        assertEquals( "Double.NEGATIVE_INFINITY", formatArgument( Double.NEGATIVE_INFINITY ) );
    }

    @Test
    void shouldFormatIndexDescriptors()
    {
        IndexDescriptor index = TestIndexDescriptorFactory.forLabel( 23, 42 );
        assertEquals( "IndexPrototype.forSchema( SchemaDescriptor.forLabel( 23, 42 ) )" +
                        ".withName( \"" + index.getName() + "\" ).materialise( " + index.getId() + " )",
                formatArgument( index ) );

        index = TestIndexDescriptorFactory.forSchema(
                SchemaDescriptor.fulltext( EntityType.NODE, new int[] {23}, new int[] {42} ) );
        assertEquals( "IndexPrototype.forSchema( SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {23}, new int[] {42} ) )" +
                        ".withName( \"" + index.getName() + "\" ).materialise( " + index.getId() + " )",
                formatArgument( index ) );

        index = TestIndexDescriptorFactory.forSchema( SchemaDescriptor.forRelType( 23, 42 ) );
        assertEquals( "IndexPrototype.forSchema( SchemaDescriptor.forRelType( 23, 42 ) )" +
                        ".withName( \"" + index.getName() + "\" ).materialise( " + index.getId() + " )",
                formatArgument( index ) );
    }

    @Test
    void shouldFormatUniquenessConstraints()
    {
        assertEquals( "ConstraintDescriptorFactory.uniqueForLabel( 23, 42 )",
                formatArgument(
                        ConstraintDescriptorFactory.uniqueForLabel( 23, 42 ) ) );
    }

    @Test
    void shouldFormatCompositeUniquenessConstraints()
    {
        assertEquals( "ConstraintDescriptorFactory.uniqueForLabel( 23, 42, 43 )",
                formatArgument( ConstraintDescriptorFactory.uniqueForLabel( 23, 42, 43 ) ) );
    }

    @Test
    void shouldFormatNodeKeyConstraints()
    {
        assertEquals( "ConstraintDescriptorFactory.nodeKeyForLabel( 23, 42, 43 )",
                formatArgument( ConstraintDescriptorFactory.nodeKeyForLabel( 23, 42, 43 ) ) );
    }

    private static String formatArgument( Object arg )
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
