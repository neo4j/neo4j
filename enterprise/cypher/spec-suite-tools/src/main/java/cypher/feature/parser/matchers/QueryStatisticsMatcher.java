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
