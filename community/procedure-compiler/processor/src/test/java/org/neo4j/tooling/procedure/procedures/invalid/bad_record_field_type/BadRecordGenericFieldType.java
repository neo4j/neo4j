/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.tooling.procedure.procedures.invalid.bad_record_field_type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class BadRecordGenericFieldType
{

    public Map<String,Integer> wrongType1;
    public List<Integer> wrongType2;
    public List<List<Map<String,Integer>>> wrongType3;
    public List<String> okType1;
    public List<Long> okType2;
    public List<Double> okType4;
    public List<Number> okType6;
    public List<Boolean> okType7;
    public List<Path> okType9;
    public List<Node> okType10;
    public List<Relationship> okType11;
    public List<Object> okType12;
    public Map<String,Object> okType13;
    public HashMap<String,Object> okType14;
    public ArrayList<Boolean> okType15;
    public ArrayList<Object> okType16;
}
