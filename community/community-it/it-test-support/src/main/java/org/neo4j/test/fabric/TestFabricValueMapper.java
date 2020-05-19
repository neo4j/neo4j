/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.fabric;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class TestFabricValueMapper extends DefaultValueMapper
{
    public TestFabricValueMapper()
    {
        super( null );
    }

    @Override
    public Node mapNode( VirtualNodeValue value )
    {
        if ( value instanceof NodeEntityWrappingNodeValue )
        { // this is the back door through which "virtual nodes" slip
            return ((NodeEntityWrappingNodeValue) value).nodeEntity();
        }
        throw new UnsupportedOperationException( "can't map VirtualNodeValue" );
    }

    @Override
    public Relationship mapRelationship( VirtualRelationshipValue value )
    {
        if ( value instanceof RelationshipEntityWrappingValue )
        { // this is the back door through which "virtual relationships" slip
            return ((RelationshipEntityWrappingValue) value).relationshipEntity();
        }
        throw new UnsupportedOperationException( "can't map VirtualRelationshipValue" );
    }
}
