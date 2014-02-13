/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

public class CommandSet
{
    private final NeoStore neoStore;
    private final Map<Long, Command.NodeCommand> nodeCommands = new TreeMap<>();
    private final ArrayList<Command.PropertyCommand> propCommands = new ArrayList<>();
    private final ArrayList<Command.RelationshipCommand> relCommands = new ArrayList<>();
    private final ArrayList<Command.SchemaRuleCommand> schemaRuleCommands = new ArrayList<>();
    private final ArrayList<Command.RelationshipGroupCommand> relGroupCommands = new ArrayList<>();
    private final ArrayList<Command.RelationshipTypeTokenCommand> relationshipTypeTokenCommands = new ArrayList<>();
    private final ArrayList<Command.LabelTokenCommand> labelTokenCommands = new ArrayList<>();
    private final ArrayList<Command.PropertyKeyTokenCommand> propertyKeyTokenCommands = new ArrayList<>();
    private Command.NeoStoreCommand neoStoreCommand;

    public CommandSet( NeoStore neoStore )
    {
        this.neoStore = neoStore;
    }

    public Map<Long, Command.NodeCommand> getNodeCommands()
    {
        return nodeCommands;
    }

    public ArrayList<Command.PropertyCommand> getPropCommands()
    {
        return propCommands;
    }

    public ArrayList<Command.RelationshipCommand> getRelCommands()
    {
        return relCommands;
    }

    public ArrayList<Command.SchemaRuleCommand> getSchemaRuleCommands()
    {
        return schemaRuleCommands;
    }

    public ArrayList<Command.RelationshipTypeTokenCommand> getRelationshipTypeTokenCommands()
    {
        return relationshipTypeTokenCommands;
    }

    public ArrayList<Command.LabelTokenCommand> getLabelTokenCommands()
    {
        return labelTokenCommands;
    }

    public ArrayList<Command.PropertyKeyTokenCommand> getPropertyKeyTokenCommands()
    {
        return propertyKeyTokenCommands;
    }

    public void generateNeoStoreCommand( NeoStoreRecord neoStoreRecord )
    {
        neoStoreCommand = new Command.NeoStoreCommand( neoStore, neoStoreRecord );
    }

    public XaCommand getNeoStoreCommand()
    {
        return neoStoreCommand;
    }

    public ArrayList<Command.RelationshipGroupCommand> getRelGroupCommands()
    {
        return relGroupCommands;
    }

    public void setNeoStoreCommand( Command.NeoStoreCommand xaCommand )
    {
        neoStoreCommand = xaCommand;
    }

    public void close()
    {
        nodeCommands.clear();
        propCommands.clear();
        propertyKeyTokenCommands.clear();
        relCommands.clear();
        schemaRuleCommands.clear();
        relationshipTypeTokenCommands.clear();
        labelTokenCommands.clear();
        relGroupCommands.clear();
        neoStoreCommand = null;
    }
}
