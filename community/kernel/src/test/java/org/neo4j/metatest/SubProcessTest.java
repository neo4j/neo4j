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
package org.neo4j.metatest;

import org.junit.Ignore;
import org.junit.Test;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.subprocess.SubProcess;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SubProcessTest
{
    private static final String MESSAGE = "message";

    @SuppressWarnings( "serial" )
    private static class TestingProcess extends SubProcess<Callable<String>, String> implements Callable<String>
    {
        private String message;
        private transient volatile boolean started = false;

        @Override
        protected void startup( String parameter )
        {
            message = parameter;
            started = true;
        }

        @Override
        public String call() throws Exception
        {
            while ( !started )
                // because all calls are asynchronous
                Thread.sleep( 1 );
            return message;
        }
    }

    @Test
    public void canInvokeSubprocessMethod() throws Exception
    {
        Callable<String> subprocess = new TestingProcess().start( MESSAGE );
        try
        {
            assertEquals( MESSAGE, subprocess.call() );
        }
        finally
        {
            SubProcess.stop( subprocess );
        }
    }

    @Ignore( "not reliable - the processes do exit though" )
    @Test
    public void subprocessShouldExitWhenParentProcessExits() throws Exception
    {
        CallbackImpl callback = new CallbackImpl();
        Object proc = new ParentProcess().start( callback );
        assertTrue( "Subprocess didn't exit properly", callback.isCalled( /*timeout:*/10, SECONDS ) );
        SubProcess.kill( proc );
    }

    private interface Callback extends Remote
    {
        void callBack() throws RemoteException;
    }

    private interface Handover extends Remote
    {
        Callback handOver() throws RemoteException;
    }

    @SuppressWarnings( "serial" )
    private static class CallbackImpl extends UnicastRemoteObject implements Callback
    {
        private volatile boolean called = false;

        protected CallbackImpl() throws RemoteException
        {
            super();
        }

        boolean isCalled( int timeout, TimeUnit unit ) throws InterruptedException
        {
            long end = System.currentTimeMillis() + unit.toMillis( timeout );
            while ( !called && System.currentTimeMillis() < end )
                Thread.sleep( 1 );
            return called;
        }

        @Override
        public void callBack()
        {
            called = true;
        }
    }

    private static class HandoverImpl extends UnicastRemoteObject implements Handover
    {
        private volatile boolean called = false;
        private final Callback callback;

        protected HandoverImpl( Callback callback ) throws RemoteException
        {
            super();
            this.callback = callback;
        }

        @Override
        public Callback handOver() throws RemoteException
        {
            called = true;
            return callback;
        }

        boolean isCalled( int timeout, TimeUnit unit ) throws InterruptedException
        {
            long end = System.currentTimeMillis() + unit.toMillis( timeout );
            while ( !called && System.currentTimeMillis() < end )
                Thread.sleep( 1 );
            return called;
        }
    }

    @SuppressWarnings( "serial" )
    private static class ParentProcess extends SubProcess<Object, Callback>
    {
        @Override
        protected void startup( Callback parameter ) throws Throwable
        {
            HandoverImpl handover = new HandoverImpl( parameter );
            new ChildProcess().start( handover );
            handover.isCalled( /*timeout:*/5, SECONDS );
            shutdown();
        }
    }

    @SuppressWarnings( "serial" )
    private static class ChildProcess extends SubProcess<Object, Handover>
    {
        private Callback callback;

        @Override
        protected synchronized void startup( Handover parameter ) throws Throwable
        {
            this.callback = parameter.handOver();
        }

        @Override
        protected synchronized void shutdown( boolean normal )
        {
            try
            {
                callback.callBack();
            }
            catch ( RemoteException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
