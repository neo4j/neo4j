/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.vagrant;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;

/*
 * The fact that this implements the graph database transaction
 * API is just because I find that funny. If it ever leads to
 * compilation problems due to changes in the transaction API,
 * just make this not implement Transaction.
 */
public class VagrantTransaction implements Transaction {

    private Vagrant vm;
    private boolean success = false;

    public VagrantTransaction(Vagrant vagrant)
    {
        this.vm = vagrant;
    }

    @Override
    public void failure()
    {
        success = false;
    }

    @Override
    public void success()
    {
        success = true;
    }

    @Override
    public void finish()
    {
        if(success) {
            vm.vagrant("sandbox commit");
        } else {
            vm.vagrant("sandbox rollback");
        }
        
        vm.vagrant("sandbox off");
    }

    @Override
    public Lock acquireReadLock(PropertyContainer arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Lock acquireWriteLock(PropertyContainer arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
