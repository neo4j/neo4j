/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.graphalgo.impl.util;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

public class DoubleEvaluator implements CostEvaluator<Double>
{
    private String costpropertyName;

    public DoubleEvaluator( String costpropertyName )
    {
        super();
        this.costpropertyName = costpropertyName;
    }

    public Double getCost( Relationship relationship, Direction direction )
    {
        Object costProp = relationship.getProperty( costpropertyName );
        if(costProp instanceof Double)
        {
            return (Double)costProp;
        } else if (costProp instanceof Integer)
        {
            return (double)(Integer)costProp;
        } else
        {
            return Double.parseDouble( costProp.toString() );
        }
    }
}
