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
package org.neo4j.server.rest.repr.formats;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import javax.ws.rs.core.MediaType;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.RepresentationFormat;

@ServiceProvider
public class JsonFormat extends RepresentationFormat {
    public JsonFormat() {
        super(MediaType.APPLICATION_JSON_TYPE);
    }

    @Override
    protected ListWriter serializeList(String type) {
        return new ListWrappingWriter(new ArrayList<>());
    }

    @Override
    protected String complete(ListWriter serializer) {
        return JsonHelper.createJsonFrom(((ListWrappingWriter) serializer).data);
    }

    @Override
    protected MappingWriter serializeMapping(String type) {
        return new MapWrappingWriter(new LinkedHashMap<>());
    }

    @Override
    protected String complete(MappingWriter serializer) {
        return JsonHelper.createJsonFrom(((MapWrappingWriter) serializer).data);
    }

    @Override
    protected String serializeValue(String type, Object value) {
        return JsonHelper.createJsonFrom(value);
    }
}
