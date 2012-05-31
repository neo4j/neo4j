/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package jmx;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.management.Descriptor;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.ha.CreateEmptyDb;
import org.neo4j.ha.Neo4jHaCluster;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class JmxDocsTest
{
    private static final String IFDEF_HTMLOUTPUT = "ifndef::nonhtmloutput[]\n";
    private static final String IFDEF_NONHTMLOUTPUT = "ifdef::nonhtmloutput[]\n";
    private static final String ENDIF = "endif::nonhtmloutput[]\n";
    private static final String BEAN_NAME0 = "name0";
    private static final String BEAN_NAME = "name";
    private static final List<String> QUERIES = Arrays.asList( new String[] { "org.neo4j:*" } );
    private static final String JAVADOC_URL = "http://components.neo4j.org/neo4j-enterprise/{neo4j-version}/apidocs/";
    private static final int EXPECTED_NUMBER_OF_BEANS = 13;
    private static final Set<String> EXCLUDES = new HashSet<String>()
    {
        {
            add( "JMX Server" );
        }
    };
    private static final Map<String, String> TYPES = new HashMap<String, String>()
    {
        {
            put( "java.lang.String", "string" );
            put( "long", "integer (64-bit, long)" );
            put( "int", "integer (32-bit, int)" );
            put( "java.util.List", "list (java.util.List)" );
            put( "java.util.Date", "date (java.util.Date)" );
        }
    };
    private static final TargetDirectory dir = TargetDirectory.forTest( JmxDocsTest.class );
    private static LocalhostZooKeeperCluster zk;
    private static HighlyAvailableGraphDatabase db;

    @BeforeClass
    public static void startDb() throws Exception
    {
        zk = LocalhostZooKeeperCluster.singleton()
                .clearDataAndVerifyConnection();
        File storeDir = dir.graphDbDir( /*clean=*/true );
        CreateEmptyDb.at( storeDir );
        db = Neo4jHaCluster.single( zk, storeDir, /*HA port:*/3377, //
                MapUtil.stringMap( "jmx.port", "9913" ) );
    }

    @AfterClass
    public static void stopDb() throws Exception
    {
        if ( db != null ) db.shutdown();
        db = null;
        dir.cleanup();
    }

    @Test
    public void dumpJmxInfo() throws Exception
    {
        StringBuilder beanList = new StringBuilder( 4096 );
        StringBuilder altBeanList = new StringBuilder( 2048 );
        altBeanList.append( IFDEF_NONHTMLOUTPUT );
        beanList.append( "[[jmx-list]]\n" + ".MBeans exposed by Neo4j\n"
                         + IFDEF_HTMLOUTPUT
                         + "[options=\"header\", cols=\"m,\"]\n" + "|===\n"
                         + "|Name|Description\n" );

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        SortedMap<String, ObjectName> neo4jBeans = new TreeMap<String, ObjectName>();

        for ( String query : QUERIES )
        {
            Set<ObjectInstance> beans = mBeanServer.queryMBeans(
                    new ObjectName( query ), null );
            for ( ObjectInstance bean : beans )
            {
                ObjectName objectName = bean.getObjectName();
                String name = objectName.getKeyProperty( BEAN_NAME );
                if ( EXCLUDES.contains( name ) )
                {
                    continue;
                }
                String name0 = objectName.getKeyProperty( BEAN_NAME0 );
                if ( name0 != null )
                {
                    name += '/' + name0;
                }
                neo4jBeans.put( name, bean.getObjectName() );
            }

        }
        assertEquals( "Sanity checking the number of beans found;",
                EXPECTED_NUMBER_OF_BEANS, neo4jBeans.size() );
        for ( Map.Entry<String, ObjectName> beanEntry : neo4jBeans.entrySet() )
        {
            ObjectName objectName = beanEntry.getValue();
            String name = beanEntry.getKey();
            Set<ObjectInstance> mBeans = mBeanServer.queryMBeans( objectName,
                    null );
            if ( mBeans.size() != 1 )
            {
                throw new IllegalStateException( "Unexpected size ["
                                                 + mBeans.size()
                                                 + "] of query result for ["
                                                 + objectName + "]." );
            }
            ObjectInstance bean = mBeans.iterator()
                    .next();
            MBeanInfo info = mBeanServer.getMBeanInfo( objectName );
            String description = info.getDescription()
                    .replace( '\n', ' ' );

            String id = getId( name );
            beanList.append( "|<<" )
                    .append( id )
                    .append( ',' )
                    .append( name )
                    .append( ">>|" )
                    .append( description )
                    .append( '\n' );

            altBeanList.append( "* <<" )
                    .append( id )
                    .append( ',' )
                    .append( name )
                    .append( ">>: " )
                    .append( description )
                    .append( '\n' );

            writeDetailsToFile( id, objectName, bean, info, description );
        }
        beanList.append( "|===\n" )
                .append( ENDIF );
        altBeanList.append( ENDIF )
                .append( "\n" );
        beanList.append( altBeanList.toString() );
        Writer fw = null;
        try
        {
            fw = AsciiDocGenerator.getFW( "target/docs/ops", "JMX List" );
            fw.write( beanList.toString() );
        }
        finally
        {
            if ( fw != null )
            {
                fw.close();
            }
        }
    }

    private String getId( String name )
    {
        return "jmx-" + name.replace( ' ', '-' )
                .replace( '/', '-' )
                .toLowerCase();
    }

    private void writeDetailsToFile( String id, ObjectName objectName,
            ObjectInstance bean, MBeanInfo info, String description )
            throws IOException
    {
        StringBuilder beanInfo = new StringBuilder( 2048 );
        String name = objectName.getKeyProperty( BEAN_NAME );
        String name0 = objectName.getKeyProperty( BEAN_NAME0 );
        if ( name0 != null )
        {
            name += "/" + name0;
        }

        MBeanAttributeInfo[] attributes = info.getAttributes();
        if ( attributes.length > 0 )
        {
            beanInfo.append( "[[" )
                    .append( id )
                    .append( "]]\n" + ".MBean " )
                    .append( name )
                    .append( " (" )
                    .append( bean.getClassName() )
                    .append( ") Attributes\n" );
            writeAttributesTable( description, beanInfo, attributes, false );
            writeAttributesTable( description, beanInfo, attributes, true );
            beanInfo.append( "\n" );
        }

        MBeanOperationInfo[] operations = info.getOperations();
        if ( operations.length > 0 )
        {
            beanInfo.append( ".MBean " )
                    .append( name )
                    .append( " (" )
                    .append( bean.getClassName() )
                    .append( ") Operations\n" );
            writeOperationsTable( beanInfo, operations, false );
            writeOperationsTable( beanInfo, operations, true );
            beanInfo.append( "\n" );
        }

        if ( beanInfo.length() > 0 )
        {
            Writer fw = null;
            try
            {
                fw = AsciiDocGenerator.getFW( "target/docs/ops", id );
                fw.write( beanInfo.toString() );
            }
            finally
            {
                if ( fw != null )
                {
                    fw.close();
                }
            }
        }
    }

    private void writeAttributesTable( String description,
            StringBuilder beanInfo, MBeanAttributeInfo[] attributes,
            boolean nonHtml )
    {
        addNonHtmlCondition( beanInfo, nonHtml );
        beanInfo.append(
                "[options=\"header\", cols=\"20m,36,20m,7,7\"]\n" + "|===\n"
                        + "|Name|Description|Type|Read|Write\n" + "5.1+^e|" )
                .append( description )
                .append( '\n' );
        for ( MBeanAttributeInfo attrInfo : attributes )
        {
            String type = getType( attrInfo.getType() );
            Descriptor descriptor = attrInfo.getDescriptor();
            type = getCompositeType( type, descriptor, nonHtml );
            beanInfo.append( '|' )
                    .append( makeBreakable( attrInfo.getName(), nonHtml ) )
                    .append( '|' )
                    .append( attrInfo.getDescription()
                            .replace( '\n', ' ' ) )
                    .append( '|' )
                    .append( type )
                    .append( '|' )
                    .append( attrInfo.isReadable() ? "yes" : "no" )
                    .append( '|' )
                    .append( attrInfo.isWritable() ? "yes" : "no" )
                    .append( '\n' );
        }
        beanInfo.append( "|===\n" );
        beanInfo.append( ENDIF );
    }

    private void addNonHtmlCondition( StringBuilder beanInfo, boolean nonHtml )
    {
        if ( nonHtml )
        {
            beanInfo.append( IFDEF_NONHTMLOUTPUT );
        }
        else
        {
            beanInfo.append( IFDEF_HTMLOUTPUT );
        }
    }

    private void writeOperationsTable( StringBuilder beanInfo,
            MBeanOperationInfo[] operations, boolean nonHtml )
    {
        addNonHtmlCondition( beanInfo, nonHtml );
        beanInfo.append( "[options=\"header\", cols=\"20m,40,20m,20m\"]\n"
                         + "|===\n"
                         + "|Name|Description|ReturnType|Signature\n" );
        for ( MBeanOperationInfo operInfo : operations )
        {
            String type = getType( operInfo.getReturnType() );
            Descriptor descriptor = operInfo.getDescriptor();
            type = getCompositeType( type, descriptor, nonHtml );
            beanInfo.append( '|' );
            beanInfo.append( operInfo.getName() );
            beanInfo.append( '|' );
            beanInfo.append( operInfo.getDescription()
                    .replace( '\n', ' ' ) );
            beanInfo.append( '|' );
            beanInfo.append( type );
            beanInfo.append( '|' );
            MBeanParameterInfo[] params = operInfo.getSignature();
            for ( int i = 0; i < params.length; i++ )
            {
                MBeanParameterInfo param = params[i];
                beanInfo.append( param.getName() );
                beanInfo.append( ':' );
                beanInfo.append( param.getType() );
                if ( i != ( params.length - 1 ) )
                {
                    beanInfo.append( ',' );
                }
            }
            beanInfo.append( '\n' );
        }
        beanInfo.append( "|===\n" );
        beanInfo.append( ENDIF );
    }

    private String getCompositeType( String type, Descriptor descriptor,
            boolean nonHtml )
    {
        String newType = type;
        if ( "javax.management.openmbean.CompositeData[]".equals( type ) )
        {
            Object originalType = descriptor.getFieldValue( "originalType" );
            if ( originalType != null )
            {
                newType = getLinkedType( getType( (String) originalType ),
                        nonHtml );
                if ( nonHtml )
                {
                    newType += " as CompositeData[]";
                }
                else
                {
                    newType += " as http://docs.oracle.com/javase/6/docs/api/javax/management/openmbean/CompositeData.html[CompositeData][]";
                }
            }
        }
        return newType;
    }

    private String getType( String type )
    {
        if ( TYPES.containsKey( type ) )
        {
            return TYPES.get( type );
        }
        else if ( type.endsWith( ";" ) )
        {
            if ( type.startsWith( "[L" ) )
            {
                return type.substring( 2, type.length() - 1 ) + "[]";
            }
            else
            {
                throw new IllegalArgumentException(
                        "Don't know how to parse this type: " + type );
            }
        }
        return type;
    }

    private String getLinkedType( String type, boolean nonHtml )
    {
        if ( !type.startsWith( "org.neo4j" ) )
        {
            if ( !type.startsWith( "java.util.List<org.neo4j." ) )
            {
                return type;
            }
            else
            {
                String typeInList = type.substring( 15, type.length() - 1 );
                return "java.util.List<" + getLinkedType( typeInList, nonHtml )
                       + ">";
            }
        }
        else if ( nonHtml )
        {
            return type;
        }
        else
        {
            StringBuilder url = new StringBuilder( 160 );
            url.append( JAVADOC_URL );
            String typeString = type;
            if ( type.endsWith( "[]" ) )
            {
                typeString = type.substring( 0, type.length() - 2 );
            }
            url.append( typeString.replace( '.', '/' ) )
                    .append( ".html[" )
                    .append( typeString )
                    .append( "]" );
            if ( type.endsWith( "[]" ) )
            {
                url.append( "[]" );
            }
            return url.toString();
        }
    }

    private String makeBreakable( String name, boolean nonHtml )
    {
        if ( nonHtml )
        {
            return name.replace( "_", "_\u200A" )
                    .replace( "NumberOf", "NumberOf\u200A" )
                    .replace( "InUse", "\u200AInUse" )
                    .replace( "Transactions", "\u200ATransactions" );
        }
        else
        {
            return name;
        }
    }
}
