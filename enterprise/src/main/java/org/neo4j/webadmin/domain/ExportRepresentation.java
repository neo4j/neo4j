/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.webadmin.domain;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.rest.domain.Representation;

/**
 * Contains a string URL, pointing to a completed export that can be downloaded.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class ExportRepresentation implements Representation
{

    public static final String EXPORT_URL_KEY = "url";

    private URI exportUri;

    public ExportRepresentation( URI exportURI )
    {
        this.exportUri = exportURI;
    }

    public Object serialize()
    {
        Map<String, Object> serial = new HashMap<String, Object>();

        serial.put( EXPORT_URL_KEY, exportUri.toString() );

        return serial;
    }
}
