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
package org.neo4j.kernel.impl.util.dbstructure;

import java.util.Iterator;

import org.neo4j.helpers.collection.Pair;

public interface DbStructureLookup
{
    Iterator<Pair<Integer, String>> labels();
    Iterator<Pair<Integer, String>> properties();
    Iterator<Pair<Integer, String>> relationshipTypes();

    Iterator<Pair<String, String[]>> knownIndices();
    Iterator<Pair<String, String[]>> knownUniqueIndices();
    Iterator<Pair<String, String[]>> knownUniqueConstraints();
    Iterator<Pair<String, String[]>> knownNodePropertyExistenceConstraints();
    Iterator<Pair<String, String[]>> knownRelationshipPropertyExistenceConstraints();
    Iterator<Pair<String, String[]>> knownNodeKeyConstraints();

    long nodesAllCardinality();
    long nodesWithLabelCardinality( int labelId );
    long cardinalityByLabelsAndRelationshipType( int fromLabelId, int relTypeId, int toLabelId );
    double indexSelectivity( int labelId, int... propertyKeyIds );
    double indexPropertyExistsSelectivity( int labelId, int... propertyKeyIds );
}
