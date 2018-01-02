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
package org.neo4j.server.rest.web;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Helps generate testable data models, using a RestfulGraphDatabase.
 *
 */
public class ModelBuilder
{
    public static DomainModel generateMatrix( RestfulGraphDatabase rgd )
    {
        String key = "key_get";
        String value = "value";

        DomainModel dm = new DomainModel();

        DomainEntity thomas = new DomainEntity();
        thomas.properties.put( "name", "Thomas Anderson" );
        thomas.location = (URI) rgd.createNode( "{\"name\":\"" + "Thomas Anderson" + "\"}" )
                .getMetadata()
                .getFirst( "Location" );
        dm.add( thomas );

        DomainEntity agent = new DomainEntity();
        agent.properties.put( "name", "Agent Smith" );
        agent.location = (URI) rgd.createNode( "{\"name\":\"" + "Agent Smith" + "\"}" )
                .getMetadata()
                .getFirst( "Location" );
        dm.add( agent );

        dm.nodeIndexName = "matrixal-nodes";
        dm.indexedNodeKeyValues.put( key, value );

        dm.indexedNodeUriToEntityMap.put(
                (URI) rgd.addToNodeIndex( dm.nodeIndexName, null, null, "{\"key\": \"" + key + "\", \"value\":\"" + value + "\", \"uri\": \"" + thomas.location + "\"}" )
                        .getMetadata()
                        .getFirst( "Location" ), thomas );
        dm.indexedNodeUriToEntityMap.put(
                (URI) rgd.addToNodeIndex( dm.nodeIndexName, null, null, "{\"key\": \"" + key + "\", \"value\":\"" + value + "\", \"uri\": \"" + agent.location + "\"}" )
                        .getMetadata()
                        .getFirst( "Location" ), agent );

        return dm;
    }

    public static class DomainEntity
    {
        public URI location;
        public Map<String, String> properties = new HashMap<String, String>();
    }

    public static class DomainModel
    {
        public Map<URI, DomainEntity> nodeUriToEntityMap = new HashMap<URI, DomainEntity>();
        String nodeIndexName = "nodes";
        public Map<String, String> indexedNodeKeyValues = new HashMap<String, String>();
        public Map<URI, DomainEntity> indexedNodeUriToEntityMap = new HashMap<URI, DomainEntity>();
        String relationshipIndexName = "relationships";
        public Map<URI, DomainEntity> indexedRelationshipUriToEntityMap = new HashMap<URI, DomainEntity>();
        public Map<String, String> indexedRelationshipKeyValues = new HashMap<String, String>();

        public void add( DomainEntity de )
        {
            nodeUriToEntityMap.put( de.location, de );
        }
    }
}
