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
package org.neo4j.kernel.impl.transaction.command;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.kernel.impl.store.record.NeoStoreRecord;

public class CommandSet
{
    private final Map<Long, org.neo4j.kernel.impl.transaction.command.Command.NodeCommand> nodeCommands = new TreeMap<>();
    private final ArrayList<org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand> propCommands = new ArrayList<>();
    private final ArrayList<org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand> relCommands = new ArrayList<>();
    private final ArrayList<org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand> schemaRuleCommands = new ArrayList<>();
    private final ArrayList<org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand> relGroupCommands = new ArrayList<>();
    private final ArrayList<org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand> relationshipTypeTokenCommands = new ArrayList<>();
    private final ArrayList<org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand> labelTokenCommands = new ArrayList<>();
    private final ArrayList<org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand> propertyKeyTokenCommands = new ArrayList<>();
    private final org.neo4j.kernel.impl.transaction.command.Command.NeoStoreCommand neoStoreCommand = new Command.NeoStoreCommand();

    public Map<Long, org.neo4j.kernel.impl.transaction.command.Command.NodeCommand> getNodeCommands()
    {
        return nodeCommands;
    }

    public ArrayList<org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand> getPropCommands()
    {
        return propCommands;
    }

    public ArrayList<org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand> getRelCommands()
    {
        return relCommands;
    }

    public ArrayList<org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand> getSchemaRuleCommands()
    {
        return schemaRuleCommands;
    }

    public ArrayList<org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand> getRelationshipTypeTokenCommands()
    {
        return relationshipTypeTokenCommands;
    }

    public ArrayList<org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand> getLabelTokenCommands()
    {
        return labelTokenCommands;
    }

    public ArrayList<org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand> getPropertyKeyTokenCommands()
    {
        return propertyKeyTokenCommands;
    }

    public void generateNeoStoreCommand( NeoStoreRecord neoStoreRecord )
    {
        neoStoreCommand.init( neoStoreRecord );
    }

    public Command.NeoStoreCommand getNeoStoreCommand()
    {
        return neoStoreCommand;
    }

    public ArrayList<org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand> getRelGroupCommands()
    {
        return relGroupCommands;
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
        neoStoreCommand.init( null );
    }
}
