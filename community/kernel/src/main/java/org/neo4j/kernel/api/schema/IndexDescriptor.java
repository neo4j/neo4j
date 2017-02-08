/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.schema;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.lang.String.format;

/**
 * Description of a single index based on one label and one or more properties.
 *
 * @see SchemaRule
 */
@Deprecated
public interface IndexDescriptor
{
    /**
     * @return the key representing the label for which this index is defined
     */
    int getLabelId();

    /**
     * @return the key representing the single property for which this index is defined if the index is a single
     * property index, or throw an exception otherwise
     */
    int getPropertyKeyId();

    /**
     * @return the keys representing the multiple properties for which this composite index is defined if the index
     * is a multi-property index, or throw an exception otherwise
     */
    int[] getPropertyKeyIds();

    /**
     * @return true if this index is a multi-property index
     */
    boolean isComposite();

    /**
     * @param tokenNameLookup table of mappings from integer keys to token names for labels and properties
     * @return a user readable description of the index using the label and property names instead of keys
     */
    String userDescription( TokenNameLookup tokenNameLookup );

    /**
     * @return the underlying descriptor used to define this index
     */
    NodePropertyDescriptor descriptor();
}
