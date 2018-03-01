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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Modes;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;

/**
 * This is a temporary implementation of the Kernel API, used to enable early testing. The plan is to merge this
 * class with org.neo4j.kernel.impl.api.Kernel.
 */
public class NewKernel implements Kernel, Modes
{
    private final StorageEngine engine;
    private final KernelToken token;
    private final InwardKernel kernel;

    private StorageStatement statement;
    private DefaultCursors cursors;

    private volatile boolean isRunning;

    public NewKernel( StorageEngine engine, KernelToken token, InwardKernel kernel )
    {
        this.engine = engine;
        this.token = token;
        this.kernel = kernel;
        this.isRunning = false;
    }

    @Override
    public CursorFactory cursors()
    {
        assert isRunning : "kernel is not running, so it is not possible to use it";
        return cursors;
    }

    @Override
    public KernelSession beginSession( LoginContext loginContext )
    {
        assert isRunning : "kernel is not running, so it is not possible to use it";
        return new KernelSession( token, kernel, loginContext );
    }

    @Override
    public Modes modes()
    {
        return this;
    }

    public void start()
    {
        statement = engine.storeReadLayer().newStatement();
        this.cursors = new DefaultCursors();
        isRunning = true;
    }

    public void stop()
    {
        if ( !isRunning )
        {
            throw new IllegalStateException( "kernel is not running, so it is not possible to stop it" );
        }
        statement.close();
        isRunning = false;
        this.cursors = null;
    }

    @Override
    public boolean twoLayerTransactionState()
    {
        return true;
    }
}
