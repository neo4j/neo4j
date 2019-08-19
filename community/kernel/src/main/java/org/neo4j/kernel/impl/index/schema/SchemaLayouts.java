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
package org.neo4j.kernel.impl.index.schema;

import java.util.StringJoiner;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Meta;
import org.neo4j.index.internal.gbptree.MetaToLayoutFactory;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;

public class SchemaLayouts implements MetaToLayoutFactory
{
    @Override
    public Layout<?,?> create( Meta meta, String targetLayout )
    {
        Layout<?,?> result = parseLayout( targetLayout );
        if ( result != null )
        {
            if ( matchingLayout( meta, result ) )
            {
                return result;
            }
            throw new RuntimeException( "Layout with identifier \"" + targetLayout + "\" did not match meta " + meta );
        }
        throw new RuntimeException( "Could not load layout for identifier \"" + targetLayout + "\". Available options are " + supportedLayouts() );
    }

    private static String supportedLayouts()
    {
        StringJoiner joiner = new StringJoiner( ", ", "[", "]" );
        joiner.add( "generic<numberOfSlots> (generic1, generic2, etc)" );
        return joiner.toString();
    }

    private static boolean matchingLayout( Meta meta, Layout layout )
    {
        try
        {
            meta.verify( layout );
            return true;
        }
        catch ( MetadataMismatchException e )
        {
            return false;
        }
    }

    private static Layout<?,?> parseLayout( String layout )
    {
        if ( layout.contains( "generic" ) )
        {
            String numberOfSlotsString = layout.replace( "generic", "" );
            int numberOfSlots = Integer.parseInt( numberOfSlotsString );
            return new GenericLayout( numberOfSlots, null );
        }
        return null;
    }
}
