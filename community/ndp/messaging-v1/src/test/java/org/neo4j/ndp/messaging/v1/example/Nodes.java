/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.messaging.v1.example;

import java.util.Collections;

import org.neo4j.graphdb.Node;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueNode;

import static java.util.Arrays.asList;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.example.Support.NO_LABELS;

public class Nodes
{
    public static final Node ALICE = new ValueNode(
            1001L,
            asList( label( "Person" ), label( "Employee" ) ),
            map( "name", "Alice", "age", 33L ) );
    public static final Node BOB = new ValueNode(
            1002L,
            asList( label( "Person" ), label( "Employee" ) ),
            map( "name", "Bob", "age", 44L ) );
    public static final Node CAROL = new ValueNode(
            1003L,
            Collections.singletonList( label( "Person" ) ),
            map( "name", "Carol" ) );
    public static final Node DAVE = new ValueNode(
            1004L,
            NO_LABELS,
            map( "name", "Dave" ) );

}
