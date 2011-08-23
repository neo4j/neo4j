/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.plugin.gremlin;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jVertex;
import com.tinkerpop.gremlin.pipes.util.Table;
import org.neo4j.server.rest.repr.*;

import java.util.ArrayList;
import java.util.List;

public class GremlinToRepresentationConverter {
    public GremlinToRepresentationConverter() {
    }

    public Representation convert(final Object data) {
        if (data instanceof Table) {
            return new GremlinTableRepresentation((Table) data);
        }
        if (data instanceof Iterable) {
            return getListRepresentation((Iterable) data);
        }
        return getSingleRepresentation(data);
    }

    Representation getListRepresentation(Iterable data) {
        final List<Representation> results = convertValuesToRepresentations(data);
        return new ListRepresentation(getType(results), results);
    }

    List<Representation> convertValuesToRepresentations(Iterable data) {
        final List<Representation> results = new ArrayList<Representation>();
        for (final Object value : data) {
            if(value instanceof Iterable) {
                convertValuesToRepresentations( (Iterable) value );
            }
            final Representation representation = getSingleRepresentation(value);
            results.add(representation);
        }
        return results;
    }

    RepresentationType getType(List<Representation> representations) {
        if (representations == null || representations.isEmpty()) return RepresentationType.STRING;
        return representations.get(0).getRepresentationType();
    }

    Representation getSingleRepresentation(Object result) {
        if (result == null) return ValueRepresentation.string("null");
        if (result instanceof Neo4jVertex) {
            return new NodeRepresentation(((Neo4jVertex) result).getRawVertex());
        } else if (result instanceof Neo4jEdge) {
            return new RelationshipRepresentation(((Neo4jEdge) result).getRawEdge());
        } else if (result instanceof Neo4jGraph) {
            return ValueRepresentation.string(((Neo4jGraph) result).getRawGraph().toString());
        } else if (result instanceof Double || result instanceof Float) {
            return ValueRepresentation.number(((Number) result).doubleValue());
        } else if (result instanceof Long || result instanceof Integer) {
            return ValueRepresentation.number(((Number) result).longValue());
        } else {
            return ValueRepresentation.string(result.toString());
        }
    }
}