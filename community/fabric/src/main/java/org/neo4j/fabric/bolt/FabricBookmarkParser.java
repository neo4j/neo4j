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
package org.neo4j.fabric.bolt;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.fabric.bookmark.BookmarkStateSerializer;

public class FabricBookmarkParser implements CustomBookmarkFormatParser
{
    @Override
    public boolean isCustomBookmark( String string )
    {
        return string.startsWith( FabricBookmark.PREFIX );
    }

    @Override
    public List<Bookmark> parse( List<String> customBookmarks )
    {
        return customBookmarks.stream().map( this::parse ).collect( Collectors.toList());
    }

    public FabricBookmark parse( String bookmarkString )
    {
        if ( !isCustomBookmark( bookmarkString ) )
        {
            throw new IllegalArgumentException( String.format( "'%s' is not a valid Fabric bookmark", bookmarkString ) );
        }

        var content = bookmarkString.substring( FabricBookmark.PREFIX.length() );

        if ( content.isEmpty() )
        {
            return new FabricBookmark( List.of(), List.of() );
        }

        return BookmarkStateSerializer.deserialize( content );
    }
}
