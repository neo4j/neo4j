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
package org.neo4j.kernel.impl.api.index;

public class UpdatesTracker
{
    private int created = 0;
    private int deleted = 0;
    private int createdDuringPopulation = 0;
    private int deletedDuringPopulation = 0;
    private boolean populationCompleted = false;

    public void increaseCreated( int num )
    {
        created += num;
    }

    public void increaseDeleted( int num )
    {
        deleted += num;
    }


    void notifyPopulationCompleted()
    {
        if ( populationCompleted )
        {
            return;
        }

        populationCompleted = true;
        createdDuringPopulation = created;
        deletedDuringPopulation = deleted;
    }

    public boolean isPopulationCompleted()
    {
        return populationCompleted;
    }

    public int created()
    {
        return created;
    }

    public int deleted()
    {
        return deleted;
    }

    public int createdDuringPopulation()
    {
        return createdDuringPopulation;
    }

    public int deletedDuringPopulation()
    {
        return deletedDuringPopulation;
    }

    public int createdAfterPopulation()
    {
        return created - createdDuringPopulation;
    }

    public int deletedAfterPopulation()
    {
        return deleted - deletedDuringPopulation;
    }

    public void add( UpdatesTracker updatesTracker )
    {
        assert isPopulationCompleted();
        assert updatesTracker.isPopulationCompleted();
        this.created += updatesTracker.created;
        this.deleted += updatesTracker.deleted;
        this.createdDuringPopulation += updatesTracker.createdDuringPopulation;
        this.deletedDuringPopulation += updatesTracker.deletedDuringPopulation;
    }

    @Override
    public String toString()
    {
        return "UpdatesTracker{" +
               "created=" + created +
               ", deleted=" + deleted +
               ", createdDuringPopulation=" + createdDuringPopulation +
               ", deletedDuringPopulation=" + deletedDuringPopulation +
               ", populationCompleted=" + populationCompleted +
               '}';
    }
}
