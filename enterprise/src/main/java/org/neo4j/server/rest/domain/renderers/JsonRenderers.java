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

package org.neo4j.server.rest.domain.renderers;

import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseRuntimeException;
import org.neo4j.server.rest.domain.Representation;
import org.neo4j.server.rest.repr.BadInputException;

import javax.ws.rs.core.MediaType;
import java.io.PrintWriter;
import java.io.StringWriter;

public enum JsonRenderers implements Renderer
{
    DEFAULT {
        @Override
        public String render( Representation... representations) throws JsonParseRuntimeException
        {
            return JsonHelper.createJsonFrom( RepresentationUtil.serialize(false, representations));
        }
    },
    ARRAY {
        @Override
        public String render(Representation... representations) throws JsonParseRuntimeException
        {
            return JsonHelper.createJsonFrom(RepresentationUtil.serialize(true, representations));
        }
    };
    
    public abstract String render(Representation... representations) throws BadInputException;

    public MediaType getMediaType() {
        return MediaType.APPLICATION_JSON_TYPE;
    }

    public String renderException(String subjectOrNull, Exception exception) {
        StringBuilder entity = new StringBuilder();
        if (subjectOrNull != null) {
            entity.append("Error: " + subjectOrNull + "\n");
        }
        entity.append(asString(exception));
        return entity.toString();
    }

    public static String asString(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        e.printStackTrace(writer);
        writer.close();
        return stringWriter.getBuffer().toString();
    }
}
