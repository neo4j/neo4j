/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.internal.event;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;

class EmptyTransactionData implements TransactionData
{
    static final TransactionData EMPTY_DATA = new EmptyTransactionData();

    private EmptyTransactionData()
    {

    }
    @Override
    public Iterable<Node> createdNodes()
    {
        return Iterables.empty();
    }

    @Override
    public Iterable<Node> deletedNodes()
    {
        return Iterables.empty();
    }

    @Override
    public boolean isDeleted( Node node )
    {
        return false;
    }

    @Override
    public Iterable<PropertyEntry<Node>> assignedNodeProperties()
    {
        return Iterables.empty();
    }

    @Override
    public Iterable<PropertyEntry<Node>> removedNodeProperties()
    {
        return Iterables.empty();
    }

    @Override
    public Iterable<LabelEntry> assignedLabels()
    {
        return Iterables.empty();
    }

    @Override
    public Iterable<LabelEntry> removedLabels()
    {
        return Iterables.empty();
    }

    @Override
    public Iterable<Relationship> createdRelationships()
    {
        return Iterables.empty();
    }

    @Override
    public Iterable<Relationship> deletedRelationships()
    {
        return Iterables.empty();
    }

    @Override
    public boolean isDeleted( Relationship relationship )
    {
        return false;
    }

    @Override
    public String username()
    {
        return StringUtils.EMPTY;
    }

    @Override
    public Map<String,Object> metaData()
    {
        return Collections.emptyMap();
    }

    @Override
    public Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties()
    {
        return Iterables.empty();
    }

    @Override
    public Iterable<PropertyEntry<Relationship>> removedRelationshipProperties()
    {
        return Iterables.empty();
    }

}
