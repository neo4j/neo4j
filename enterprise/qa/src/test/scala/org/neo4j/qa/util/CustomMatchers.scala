package org.neo4j.qa.util

import org.scalatest.matchers.{MatchResult, Matcher}

trait CustomMatchers {
  class FileExistsMatcher extends Matcher[java.io.File] {

    def apply(left: java.io.File) = {

      val fileOrDir = if (left.isFile) "file" else "directory"

      val failureMessageSuffix =
        fileOrDir + " named " + left.getAbsolutePath + " did not exist"

      val negatedFailureMessageSuffix =
        fileOrDir + " named " + left.getAbsolutePath + " existed"

      MatchResult(
        left.exists,
        "The " + failureMessageSuffix,
        "The " + negatedFailureMessageSuffix,
        "the " + failureMessageSuffix,
        "the " + negatedFailureMessageSuffix
      )
    }
  }

  val exist = new FileExistsMatcher
}

object CustomMatchers extends CustomMatchers

