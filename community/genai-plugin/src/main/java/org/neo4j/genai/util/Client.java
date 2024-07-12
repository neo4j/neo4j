/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.genai.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import org.eclipse.collections.api.multimap.Multimap;

public interface Client {
    InputStream sendRequest(URI uri, Multimap<String, String> propertiesHttpClient, Payload payload)
            throws IOException, GenAIProcedureException;

    interface Payload {
        void writeTo(Writer writer) throws IOException;
    }
}
