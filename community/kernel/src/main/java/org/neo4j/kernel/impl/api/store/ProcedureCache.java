/*
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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.function.Function;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedures.ProcedureSignature.ProcedureName;
import org.neo4j.kernel.api.procedures.ProcedureSource;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.procedure.CompiledProcedure;
import org.neo4j.kernel.procedure.ProcedureCompiler;

import static org.neo4j.helpers.collection.Iterables.map;

/**
 * A temporary in-memory storage for procedures, until we build "real" storage for them in 3.0.
 * TODO!
 *
 * This class is thread-safe, but uses very little synchronization. The only synchronization is on the 'procedures' map on 'put' and 'remove'.
 * If two threads retrieve the same uncompiled procedure, we will compile it twice and one will overwrite the other - but the outcome of both compilations
 * is the same, so the overwriting makes no difference. Hence, no synchronization is used on the read or compile paths.
 */
public class ProcedureCache
{
    private static final CacheEntry NONE = new CacheEntry( null );

    private final Map<ProcedureName,CacheEntry> procedures = new CopyOnWriteHashMap<>();
    private final ProcedureCompiler compiler;
    private final Function<CacheEntry,ProcedureSource> getSource = new Function<CacheEntry,ProcedureSource>()
    {
        @Override
        public ProcedureSource apply( CacheEntry pair )
        {
            return pair.source;
        }
    };

    static class CacheEntry
    {
        private final ProcedureSource source;
        private CompiledProcedure compiled;

        CacheEntry( ProcedureSource source )
        {
            this.source = source;
        }
    }

    public ProcedureCache( ProcedureCompiler compiler )
    {
        this.compiler = compiler;
    }

    public Iterator<ProcedureSource> getAll()
    {
        return map( getSource, procedures.values().iterator());
    }

    public ProcedureSource getSource( ProcedureName name )
    {
        return procedures.getOrDefault( name, NONE ).source;
    }

    public CompiledProcedure getCompiled( ProcedureName name ) throws ProcedureException
    {
        CacheEntry entry = procedures.get( name );
        if( entry == null )
        {
            throw new ProcedureException( Status.Statement.ProcedureError, "No such procedure, `%s`.", name );
        }

        if( entry.compiled == null )
        {
            entry.compiled = compiler.compile( entry.source );
        }
        return entry.compiled;
    }

    // TODO: This is temporary, to be replaced by the regular tx log/store layer stuff
    public void createProcedure( ProcedureSource source )
    {
        assert !procedures.containsKey( source.signature().name() );
        procedures.put( source.signature().name(), new CacheEntry( source ) );
    }

    public void dropProcedure( ProcedureSource procedureSource )
    {
        assert procedures.containsKey( procedureSource.signature().name() );
        procedures.remove( procedureSource.signature().name() );
    }
}
