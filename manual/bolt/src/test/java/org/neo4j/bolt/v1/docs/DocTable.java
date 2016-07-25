/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.docs;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.neo4j.bolt.v1.docs.DocPartParser.Decoration.withDetailedExceptions;

/** A table from an asciidoc file */
public class DocTable implements Iterable<DocTable.Row>
{
    public static DocPartParser<DocTable> table =
        withDetailedExceptions( DocTable.class, ( fileName, title, s ) -> new DocTable( s )
        );

    private final List<Row> rows;

    public static class Row
    {
        private final Elements cells;

        public Row( Element htmlRow )
        {
            this.cells = htmlRow.select( "td" );
        }

        String get( int columnIndex )
        {
            return cells.get( columnIndex ).text();
        }

        int numColumns()
        {
            return cells.size();
        }
    }

    public DocTable( Element htmlTable )
    {
        if ( htmlTable == null || !htmlTable.tagName().equals( "table" ) )
        {
            throw new RuntimeException( "Expected a 'table' element, but got: " + htmlTable );
        }
        this.rows = new ArrayList<>();

        for ( Element tr : htmlTable.select( "tr" ) )
        {
            if ( tr.select( "td" ).size() > 0 )
            {
                rows.add( new Row( tr ) );
            }
        }
    }

    @Override
    public Iterator<Row> iterator()
    {
        return rows.iterator();
    }
}
