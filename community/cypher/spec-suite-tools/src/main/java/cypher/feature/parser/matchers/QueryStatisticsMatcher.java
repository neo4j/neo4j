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
package cypher.feature.parser.matchers;

import org.neo4j.graphdb.QueryStatistics;

public class QueryStatisticsMatcher implements Matcher<QueryStatistics>
{
    private int nodesCreated;
    private int nodesDeleted;
    private int relationshipsCreated;
    private int relationshipsDeleted;
    private int labelsCreated;
    private int labelsDeleted;
    private int propertiesCreated;
    private int propertiesDeleted;

    @Override
    public boolean matches( QueryStatistics value )
    {
        return value.getNodesCreated() == nodesCreated
               && value.getNodesDeleted() == nodesDeleted
               && value.getRelationshipsCreated() == relationshipsCreated
               && value.getRelationshipsDeleted() == relationshipsDeleted
               && value.getLabelsAdded() == labelsCreated
               && value.getLabelsRemoved() == labelsDeleted
               && value.getPropertiesSet() == (propertiesCreated + propertiesDeleted);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "QueryStatisticsMatcher(" );
        sb.append( "+nodes=" ).append( nodesCreated );
        sb.append( ",-nodes=" ).append( nodesDeleted );
        sb.append( ",+rels=" ).append( relationshipsCreated );
        sb.append( ",-rels=" ).append( relationshipsDeleted );
        sb.append( ",+labels=" ).append( labelsCreated );
        sb.append( ",-labels=" ).append( labelsDeleted );
        sb.append( ",+props=" ).append( propertiesCreated );
        sb.append( ",-props=" ).append( propertiesDeleted );
        return sb.append( ")" ).toString();
    }

    public void setNodesCreated( int nodesCreated )
    {
        this.nodesCreated = nodesCreated;
    }

    public void setNodesDeleted( int nodesDeleted )
    {
        this.nodesDeleted = nodesDeleted;
    }

    public void setRelationshipsCreated( int relationshipsCreated )
    {
        this.relationshipsCreated = relationshipsCreated;
    }

    public void setRelationshipsDeleted( int relationshipsDeleted )
    {
        this.relationshipsDeleted = relationshipsDeleted;
    }

    public void setLabelsCreated( int labelsCreated )
    {
        this.labelsCreated = labelsCreated;
    }

    public void setLabelsDeleted( int labelsDeleted )
    {
        this.labelsDeleted = labelsDeleted;
    }

    public void setPropertiesCreated( int propertiesCreated )
    {
        this.propertiesCreated = propertiesCreated;
    }

    public void setPropertiesDeleted( int propertiesDeleted )
    {
        this.propertiesDeleted = propertiesDeleted;
    }
}
