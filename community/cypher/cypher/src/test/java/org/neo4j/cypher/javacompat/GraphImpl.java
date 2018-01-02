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
package org.neo4j.cypher.javacompat;

import java.lang.annotation.Annotation;

import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.REL;


public class GraphImpl implements Graph {

    private final String[] value;

    public GraphImpl(String[] value){
        this.value = value;
        
    }
    @Override
    public Class<? extends Annotation> annotationType()
    {
        return null;
    }

    @Override
    public String[] value()
    {
        return value;
    }

    @Override
    public NODE[] nodes()
    {
        return new NODE[]{};
    }

    @Override
    public REL[] relationships()
    {
        return new REL[]{};
    }

    @Override
    public boolean autoIndexNodes()
    {
        return true;
    }

    @Override
    public boolean autoIndexRelationships()
    {
        return true;
    }
}
