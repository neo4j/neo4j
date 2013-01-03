/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import com.google.gson.Gson;

/**
* @author mh
* @since 08.04.12
*/
public class GeoffService {
    private final GraphDatabaseService gdb;

    public GeoffService(GraphDatabaseService gdb) {
        this.gdb = gdb;
    }

    private Map<String, Node> geoffNodeParams() {
        Map<String, Node> result = new HashMap<String, Node>();
        for (Node node : GlobalGraphOperations.at(gdb).getAllNodes()) {
            result.put("(" + node.getId() + ")", node);
        }
        return result;
    }

    public String toGeoff() {
        StringBuilder sb = new StringBuilder();
        appendNodes(sb);
        appendRelationships(sb);
        return sb.toString();
    }

    private void appendRelationships(StringBuilder sb) {
        for (Node node : GlobalGraphOperations.at(gdb).getAllNodes()) {
            for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                formatNode(sb, rel.getStartNode());
                sb.append("-[:").append(rel.getType().name()).append("]->");
                formatNode(sb, rel.getEndNode());
                formatProperties(sb, rel);
                sb.append("\n");
            }
        }
    }

    private void appendNodes(StringBuilder sb) {
        for (Node node : GlobalGraphOperations.at(gdb).getAllNodes()) {
            formatNode(sb, node);
            formatProperties(sb, node);
            sb.append("\n");
        }
    }

    private void formatNode(StringBuilder sb, Node n) {
        sb.append("(").append(n.getId()).append(")");
    }


    private void formatProperties(StringBuilder sb, PropertyContainer pc) {
        sb.append(" ");
        sb.append(new Gson().toJson(toMap(pc)));
    }
    Map<String, Object> toMap(PropertyContainer pc) {
        Map<String, Object> result = new TreeMap<String, Object>();
        for (String prop : pc.getPropertyKeys()) {
            result.put(prop, pc.getProperty(prop));
        }
        return result;
    }
}
