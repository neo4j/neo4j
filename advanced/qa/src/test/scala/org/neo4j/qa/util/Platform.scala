package org.neo4j.qa.util

/**
 * Simplified notion of a host platform -- windows, unix, or mac.
 */
object Platform extends Enumeration
{
  type Platform = Value

  val WindowsPlatformRE = """.*([Ww]in).*""".r
  val MacPlatformRE = """.*([mM]ac).*""".r
  val UnixPlatformRE = """.*(n[iu]x).*""".r

  val Windows = Value("windows")
  val Unix = Value("unix")
  val Unknown = Value("unknown")

  def current =
  {
    System.getProperty("os.name") match
    {
      case WindowsPlatformRE(_) => Windows
      case MacPlatformRE(_) => Unix
      case UnixPlatformRE(_) => Unix
      case _ => Unknown
    }
  }
}



