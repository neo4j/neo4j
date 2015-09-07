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
package org.neo4j.kernel.procedure.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedures.ProcedureSource;
import org.neo4j.kernel.procedure.CompiledProcedure;
import org.neo4j.kernel.procedure.LanguageHandler;
import org.neo4j.kernel.procedure.ProcedureCompiler;

public class StandardProcedureCompiler implements ProcedureCompiler
{
    private Map<String,LanguageHandler> languageHandlers = new ConcurrentHashMap<>();

    @Override
    public void addLanguageHandler( String language, LanguageHandler handler )
    {
        if ( languageHandlers.containsKey( language ) )
        {
            throw new IllegalArgumentException( String.format( "Language %s already registered", language ) );
        }

        languageHandlers.put( language, handler );
    }

    @Override
    public CompiledProcedure compile( ProcedureSource source ) throws ProcedureException
    {
        LanguageHandler language = languageHandlers.get( source.language() );
        if(language == null)
        {
            throw new ProcedureException( Status.Statement.ProcedureError, "The procedure `%s` cannot be compiled, " +
                                                                           "no language handler for `%s` registered.", source.signature(), source.language() );
        }
        return language.compile( source );
    }
}
