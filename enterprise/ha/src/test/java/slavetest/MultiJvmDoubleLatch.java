/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package slavetest;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.CountDownLatch;

public class MultiJvmDoubleLatch extends UnicastRemoteObject implements DoubleLatch
{
    private transient final CountDownLatch first = new CountDownLatch( 1 );
    private transient final CountDownLatch second = new CountDownLatch( 1 );
    
    protected MultiJvmDoubleLatch() throws RemoteException
    {
        super();
    }

    public void countDownFirst() throws RemoteException
    {
        first.countDown();
    }

    public void awaitFirst() throws RemoteException
    {
        await( first );
    }

    public void countDownSecond() throws RemoteException
    {
        second.countDown();
    }

    public void awaitSecond() throws RemoteException
    {
        await( second );
    }

    private void await( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            e.printStackTrace();
        }
    }
}
