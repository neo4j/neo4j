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
package org.neo4j.server.http.cypher.format;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public enum DefaultJsonFactory
{
    INSTANCE( new JsonFactory().disable( JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM ) );

    private final JsonFactory value;

    DefaultJsonFactory( JsonFactory value )
    {
        this.value = value;
    }

    /**
     * @return A blueprint of the {@link JsonFactory} to be used by both the JSON input and output.
     */
    public JsonFactory get()
    {
        return value;
    }
}
