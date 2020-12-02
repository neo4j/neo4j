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
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static java.lang.String.format;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.QUERY_RELS;

class RelationshipFulltextIndexTransactionStateTest extends FulltextIndexTransactionStateTest
{
    @Override
    void createIndex( Transaction tx )
    {
        createSimpleRelationshipIndex( tx );
    }

    @Override
    long createEntityWithProperty( Transaction tx, String propertyValue )
    {
        Node node = tx.createNode();
        Relationship rel = node.createRelationshipTo( node, REL );
        rel.setProperty( PROP, propertyValue );
        return rel.getId();
    }

    @Override
    void assertQueryFindsIds( Transaction tx, String query, long... ids )
    {
        assertQueryFindsIds( tx, false, "rels", query, ids );
    }

    @Override
    void deleteEntity( Transaction tx, long entityId )
    {
        tx.getRelationshipById( entityId ).delete();
    }

    @Override
    Result queryIndex( Transaction tx, String propertyValue )
    {
        return tx.execute( format( QUERY_RELS, "rels", propertyValue ) );
    }
}
