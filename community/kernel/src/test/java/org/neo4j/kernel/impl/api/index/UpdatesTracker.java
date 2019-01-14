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
package org.neo4j.kernel.impl.api.index;

public class UpdatesTracker
{
    private int created;
    private int updated;
    private int deleted;
    private int createdDuringPopulation;
    private int updatedDuringPopulation;
    private int deletedDuringPopulation;
    private boolean populationCompleted;

    public void increaseCreated( int num )
    {
        created += num;
    }

    public void increaseDeleted( int num )
    {
        deleted += num;
    }

    public void increaseUpdated( int num )
    {
        updated += num;
    }

    void notifyPopulationCompleted()
    {
        if ( populationCompleted )
        {
            return;
        }

        populationCompleted = true;
        createdDuringPopulation = created;
        updatedDuringPopulation = updated;
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

    public int getUpdated()
    {
        return updated;
    }

    public int createdDuringPopulation()
    {
        return createdDuringPopulation;
    }

    public int deletedDuringPopulation()
    {
        return deletedDuringPopulation;
    }

    public int getUpdatedDuringPopulation()
    {
        return updatedDuringPopulation;
    }

    public int createdAfterPopulation()
    {
        return created - createdDuringPopulation;
    }

    public int deletedAfterPopulation()
    {
        return deleted - deletedDuringPopulation;
    }

    public int updatedAfterPopulation()
    {
        return updated - updatedDuringPopulation;
    }

    public void add( UpdatesTracker updatesTracker )
    {
        assert isPopulationCompleted();
        assert updatesTracker.isPopulationCompleted();
        this.created += updatesTracker.created;
        this.deleted += updatesTracker.deleted;
        this.updated += updatesTracker.updated;
        this.createdDuringPopulation += updatesTracker.createdDuringPopulation;
        this.updatedDuringPopulation += updatesTracker.updatedDuringPopulation;
        this.deletedDuringPopulation += updatesTracker.deletedDuringPopulation;
    }

    @Override
    public String toString()
    {
        return "UpdatesTracker{" +
               "created=" + created +
               ", deleted=" + deleted +
               ", createdDuringPopulation=" + createdDuringPopulation +
               ", updatedDuringPopulation=" + updatedDuringPopulation +
               ", deletedDuringPopulation=" + deletedDuringPopulation +
               ", createdAfterPopulation=" + createdAfterPopulation() +
               ", updatedAfterPopulation=" + updatedAfterPopulation() +
               ", deletedAfterPopulation=" + deletedAfterPopulation() +
               ", populationCompleted=" + populationCompleted +
               '}';
    }
}
