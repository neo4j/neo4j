/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.availability;

import java.util.Objects;

public class DescriptiveAvailabilityRequirement implements AvailabilityRequirement
{
    private final String requirementDescription;

    public DescriptiveAvailabilityRequirement( String description )
    {
        this.requirementDescription = description;
    }

    @Override
    public String description()
    {
        return requirementDescription;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        AvailabilityRequirement that = (AvailabilityRequirement) o;

        return Objects.equals( requirementDescription, that.description() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( requirementDescription );
    }
}
