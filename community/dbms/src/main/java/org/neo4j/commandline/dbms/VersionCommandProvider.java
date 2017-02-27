package org.neo4j.commandline.dbms;

import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

public class VersionCommandProvider extends AdminCommand.Provider
{

    public VersionCommandProvider()
    {
        super( "version" );
    }

    @Override
    public Arguments allArguments()
    {
        return VersionCommand.arguments();
    }

    @Override
    public String summary()
    {
        return "Check the version of a Neo4j database store.";
    }

    @Override
    public String description()
    {
        return "Checks the version of a Neo4j database store. Note that this command expects a path to a store " +
                "directory, for example --store=data/databases/graph.db.";
    }

    @Override
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new VersionCommand( outsideWorld::stdOutLine );
    }
}
