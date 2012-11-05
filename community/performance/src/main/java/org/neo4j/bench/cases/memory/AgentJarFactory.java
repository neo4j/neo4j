package org.neo4j.bench.cases.memory;

import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Creates jar files with appropriate agent settings for a given agent class.
 */
public class AgentJarFactory
{

    private static String PREMAIN_KEY = "Premain-Class";
    private static String AGENT_KEY   = "Agent-Class";

    public String createPremainAgentJar( Class<?> agentClass )
    {
        return createAgentJar( agentClass, PREMAIN_KEY );
    }

    public String createAttachableAgentJar( Class<?> agentClass )
    {
        return createAgentJar( agentClass, AGENT_KEY );
    }

    private String createAgentJar( Class<?> agentClass, String manifestHeader )
    {
        String jarFile = codeSourceOf( agentClass );
        try
        {
            verifyAgentJar( jarFile, manifestHeader );
        }
        catch ( IllegalStateException e )
        {
            Manifest manifest = new Manifest();
            manifest.getMainAttributes().put( new Attributes.Name( manifestHeader ), agentClass.getName() );
            return new JarCreator( manifest ).createTemporaryJar( jarFile );
        }
        return jarFile;
    }

    private String verifyAgentJar( String jarPath, String manifestHeader )
    {
        try
        {
            JarFile jarFile = new JarFile( jarPath );
            if ( jarFile.getManifest().getMainAttributes().get( new Attributes.Name( manifestHeader ) ) == null )
            {
                throw new IllegalStateException( jarPath + " does not specify an Agent-Class" );
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( jarPath + " does not represent a jar file", e );
        }
        return jarPath;
    }

    private String codeSourceOf( Class<?> type )
    {
        return type.getProtectionDomain().getCodeSource().getLocation().getPath();
    }

}
