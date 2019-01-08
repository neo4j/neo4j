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
package org.neo4j.tooling.procedure.procedures.valid;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class Records
{

    public static class LongWrapper
    {
        public final Long value;

        public LongWrapper( Long value )
        {
            this.value = value;
        }
    }

    public static class SimpleTypesWrapper
    {
        public String field01 = "string";
        public long field02 = 2;
        public Long field03 = 3L;
        public Number field04 = 4.0;
        public Boolean field05 = true;
        public boolean field06 = true;
        public Object field07;
        public Node field08;
        public Path field09;
        public Relationship field10;
    }

    public static class GenericTypesWrapper
    {
        public List<String> field01;
        public List<Long> field03;
        public List<Number> field04;
        public List<Boolean> field05;
        public List<Object> field07;
        public List<Node> field08;
        public List<Path> field09;
        public List<Relationship> field10;
        public Map<String,String> field11;
        public Map<String,Long> field13;
        public Map<String,Number> field14;
        public Map<String,Boolean> field15;
        public Map<String,Object> field17;
        public Map<String,Node> field18;
        public Map<String,Path> field19;
        public Map<String,Relationship> field20;
        public List<List<Relationship>> field21;
        public List<Map<String,Relationship>> field22;
    }
}
