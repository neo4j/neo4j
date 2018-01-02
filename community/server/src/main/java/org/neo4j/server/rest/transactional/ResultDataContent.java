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
package org.neo4j.server.rest.transactional;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

public enum ResultDataContent
{
    row
    {
        @Override
        public ResultDataContentWriter writer( URI baseUri )
        {
            return new RowWriter();
        }
    },
    graph
    {
        @Override
        public ResultDataContentWriter writer( URI baseUri )
        {
            return new GraphExtractionWriter();
        }
    },
    rest
    {
        @Override
        public ResultDataContentWriter writer( URI baseUri )
        {
            return new RestRepresentationWriter( baseUri );
        }
    };

    public abstract ResultDataContentWriter writer( URI baseUri );

    public static ResultDataContent[] fromNames( List<?> names )
    {
        if ( names == null || names.isEmpty() )
        {
            return null;
        }
        ResultDataContent[] result = new ResultDataContent[names.size()];
        Iterator<?> name = names.iterator();
        for ( int i = 0; i < result.length; i++ )
        {
            Object contentName = name.next();
            if ( contentName instanceof String )
            {
                try
                {
                    result[i] = valueOf( ((String) contentName).toLowerCase() );
                }
                catch ( IllegalArgumentException e )
                {
                    throw new IllegalArgumentException( "Invalid result data content specifier: " + contentName );
                }
            }
            else
            {
                throw new IllegalArgumentException( "Invalid result data content specifier: " + contentName );
            }
        }
        return result;
    }
}
