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
package org.neo4j.server.web;

import javax.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HttpHeaderUtils {
    private static final String UTF8 = "UTF-8";
    public static final Map<String, String> CHARSET = Collections.singletonMap("charset", UTF8);

    public static MediaType mediaTypeWithCharsetUtf8(String mediaType)
    {
        return new MediaType(mediaType, null, CHARSET);
    }

    public static MediaType mediaTypeWithCharsetUtf8(MediaType mediaType)
    {
        Map<String, String> parameters = mediaType.getParameters();
        if (parameters.isEmpty())
        {
            return new MediaType(mediaType.getType(), mediaType.getSubtype(), CHARSET);
        }
        if (parameters.containsKey("charset"))
        {
            return mediaType;
        }
        Map<String, String> paramsWithCharset = new HashMap<String, String>(parameters);
        paramsWithCharset.putAll(CHARSET);
        return new MediaType(mediaType.getType(), mediaType.getSubtype(), paramsWithCharset);
    }
}
