/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell.cli;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import java.util.Map;

import org.neo4j.shell.ParameterMap;
import org.neo4j.shell.exception.ParameterException;
import org.neo4j.shell.util.ParameterSetter;

/**
 * Action that adds arguments to a ParameterMap. This action always consumes an argument.
 */
public class AddParamArgumentAction extends ParameterSetter<ArgumentParserException> implements ArgumentAction
{
    /**
     * @param parameterMap the ParameterMap to add parameters to.
     */
    AddParamArgumentAction( ParameterMap parameterMap )
    {
        super( parameterMap );
    }

    @Override
    public void run( ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value ) throws ArgumentParserException
    {
        try
        {
            execute( value.toString() );
        }
        catch ( Exception e )
        {
            throw new ArgumentParserException( e.getMessage(), e, parser );
        }
    }

    @Override
    public void onAttach( Argument arg )
    {

    }

    @Override
    public boolean consumeArgument()
    {
        return true;
    }

    @Override
    protected void onWrongUsage()
    {
        throw new IllegalArgumentException( "Incorrect usage.\nusage: --param  \"name => value\"" );
    }

    @Override
    protected void onWrongNumberOfArguments()
    {
        throw new IllegalArgumentException( "Incorrect number of arguments.\nusage: --param  \"name => value\"" );
    }

    @Override
    protected void onParameterException( ParameterException e )
    {
        throw new RuntimeException( e.getMessage(), e );
    }
}
