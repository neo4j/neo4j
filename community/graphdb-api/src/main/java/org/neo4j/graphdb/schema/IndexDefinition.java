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
package org.neo4j.graphdb.schema;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.index.IndexManager;

/**
 * Definition for an index
 * 
 * NOTE: This is part of the index API introduced in Neo4j 2.0.
 * The legacy index API lives in {@link IndexManager}.
 */
public interface IndexDefinition
{
    /**
     * @return the {@link Label label} this index definition is associated with.
     */
    Label getLabel();
    
    /**
     * @return the property keys this index was created on.
     */
    Iterable<String> getPropertyKeys();
    
    /**
     * Drops this index. {@link Schema#getIndexes(Label)} will no longer include this index
     * and any related background jobs and files will be stopped and removed.
     */
    void drop();

    /**
     * @return {@code true} if this index is created as a side effect of the creation of a uniqueness constraint.
     */
    boolean isConstraintIndex();
}
