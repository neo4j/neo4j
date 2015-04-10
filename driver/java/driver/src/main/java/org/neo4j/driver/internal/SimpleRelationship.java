/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.Identity;
import org.neo4j.driver.Relationship;
import org.neo4j.driver.Value;

/**
 * {@link org.neo4j.driver.Relationship} implementation that directly contains type and properties
 * along with {@link org.neo4j.driver.Identity} values for start and end nodes.
 */
public class SimpleRelationship extends SimpleEntity implements Relationship
{
    private final Identity start;
    private final Identity end;
    private final String type;

    public SimpleRelationship( String id, String start, String end, String type )
    {
        this( Identities.identity( id ), Identities.identity( start ),
                Identities.identity( end ), type,
                Collections.<String,Value>emptyMap() );
    }

    public SimpleRelationship( String id, String start, String end, String type,
            Map<String,Value> properties )
    {
        this( Identities.identity( id ), Identities.identity( start ),
                Identities.identity( end ), type, properties );
    }

    public SimpleRelationship( Identity id, Identity start, Identity end, String type,
            Map<String,Value> properties )
    {
        super( id, properties );
        this.start = start;
        this.end = end;
        this.type = type;
    }

    @Override
    public Identity start()
    {
        return start;
    }

    @Override
    public Identity end()
    {
        return end;
    }

    @Override
    public String type()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "Relationship{" + super.toString() + ", " +
               "start=" + start +
               ", end=" + end +
               ", type='" + type + '\'' +
               '}';
    }
}
