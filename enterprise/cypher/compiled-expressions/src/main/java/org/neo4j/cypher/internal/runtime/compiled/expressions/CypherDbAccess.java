/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.opencypher.v9_0.util.CypherTypeException;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This class contains static helper methods for expressions interacting with the database
 */
@SuppressWarnings( "unused" )
public final class CypherDbAccess
{
    private CypherDbAccess()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    //data access
    public static Value nodeProperty( Transaction tx, long node, int property )
    {
        CursorFactory cursors = tx.cursors();
        try ( NodeCursor nodes = cursors.allocateNodeCursor();
              PropertyCursor properties = cursors.allocatePropertyCursor() )
        {
            tx.dataRead().singleNode( node, nodes );
            if ( nodes.next() )
            {
                nodes.properties( properties );
                return property( properties, property );
            }
            return Values.NO_VALUE;
        }
    }

    public static Value relationshipProperty( Transaction tx, long relationship, int property )
    {
        CursorFactory cursors = tx.cursors();
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor();
              PropertyCursor properties = cursors.allocatePropertyCursor() )
        {
            tx.dataRead().singleRelationship( relationship, relationships );
            if ( relationships.next() )
            {
                relationships.properties( properties );
                return property( properties, property );
            }
            return Values.NO_VALUE;
        }
    }

    private static Value property( PropertyCursor properties, int property )
    {
        while ( properties.next() )
        {
            if ( properties.propertyKey() == property )
            {
                return properties.propertyValue();
            }
        }
        return Values.NO_VALUE;
    }
}
