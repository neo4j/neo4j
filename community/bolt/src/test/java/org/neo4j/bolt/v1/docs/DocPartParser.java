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
package org.neo4j.bolt.v1.docs;

import org.jsoup.nodes.Element;

import static java.lang.String.format;

public interface DocPartParser<RESULT>
{
    RESULT parse( String fileName, String title, Element el );

    public static class Decoration
    {
        private Decoration()
        {
            throw new UnsupportedOperationException();
        }

        public static <RESULT> DocPartParser<RESULT> withDetailedExceptions( Class<RESULT> clazz, DocPartParser<RESULT> delegate )
        {
            return new DocPartParser<RESULT>()
            {
                @Override
                public RESULT parse( String fileName, String title, Element el )
                {
                    try
                    {
                        return delegate.parse( fileName, title, el );
                    }
                    catch ( RuntimeException e )
                    {
                        throw new IllegalArgumentException(
                                format( "%s[%s]: Couldn't parse %s element", fileName, title, clazz.getSimpleName() )
                        );
                    }
                }
            };
        }
    }
}
