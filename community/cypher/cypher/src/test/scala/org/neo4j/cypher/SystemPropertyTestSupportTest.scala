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
package org.neo4j.cypher

import java.util.Properties

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.{CypherFunSuite, CypherTestSupport}

class SystemPropertyTestSupportTest extends CypherFunSuite
{

  trait SystemPropertyTestSupportFixture extends CypherTestSupport with SystemPropertyTestSupport
  {
    val systemProperties = new Properties( )

    override def getSystemProperty( propertyKey: String ): (String, String) =
      (propertyKey, systemProperties.getProperty( propertyKey ))

    override def setSystemProperty( property: (String, String) ): (String, String) = property match
    {
      case (k, v) => (k, stringValue( systemProperties.setProperty( k, v ) ))
    }

    private def stringValue( value: AnyRef ) = if ( null == value )
    {
      null
    } else
    {
      value.toString
    }
  }

  test( "should get system properties" )
  {
    (new SystemPropertyTestSupportFixture
    {
      def apply( )
      {
        setSystemProperty( "os.name" -> "Linux" )
        getSystemProperty( "os.name" ) should equal( ("os.name", "Linux") )
      }
    })( )
  }

  test( "should return previous value when setting system properties" )
  {
    (new SystemPropertyTestSupportFixture
    {
      def apply( )
      {
        setSystemProperty( "os.name" -> "Linux" )
        setSystemProperty( "os.name" -> "Mac OS" ) should equal( ("os.name", "Linux") )
      }
    })( )
  }

  test( "should shadow system properties" )
  {
    (new SystemPropertyTestSupportFixture
    {
      def apply( )
      {
        setSystemProperty( "os.name" -> "Linux" )
        withSystemProperties( "os.name" -> "Windows" )
        {
          getSystemProperty( "os.name" ) should equal( ("os.name", "Windows") )
        }
      }
    })( )
  }

  test( "should restore system properties" )
  {
    (new SystemPropertyTestSupportFixture
    {
      def apply( )
      {
        setSystemProperty( "os.name" -> "Linux" )
        withSystemProperties( "os.name" -> "Windows" )
        {
          setSystemProperty( "os.name" -> "Mac OS" )
        }
        getSystemProperty( "os.name" ) should equal( ("os.name", "Linux") )
      }
    })( )
  }
}
