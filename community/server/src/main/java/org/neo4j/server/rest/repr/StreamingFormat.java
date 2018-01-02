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
package org.neo4j.server.rest.repr;

import org.neo4j.helpers.collection.MapUtil;

import javax.ws.rs.core.MediaType;
import java.io.OutputStream;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public interface StreamingFormat {
    String STREAM_HEADER = "X-Stream";
    MediaType MEDIA_TYPE = new MediaType( APPLICATION_JSON_TYPE.getType(),
            APPLICATION_JSON_TYPE.getSubtype(), MapUtil.stringMap("stream", "true", "charset", "UTF-8") );

    RepresentationFormat writeTo(OutputStream output);
}
