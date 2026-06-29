/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util

import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.BoxedBlockPositionInSequence.Standalone
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.ShrinkingMode.ShrinkAtBegin
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.ShrinkingMode.ShrinkAtEnd
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.ShrinkingMode.ShrinkInMiddle

trait Prefixable[+T] {
  def addLinePrefix(prefix: String): T = addLinePrefix(StrLiteral(prefix))
  def addLinePrefix(prefix: Span): T = addLinePrefixAndSuffix(EpsilonSpan, prefix)
  def addLineSuffix(suffix: String): T = addLinePrefix(StrLiteral(suffix))
  def addLineSuffix(suffix: Span): T = addLinePrefixAndSuffix(suffix, EpsilonSpan)

  def addLinePrefixAndSuffix(prefix: String, suffix: String): T =
    addLinePrefixAndSuffix(StrLiteral(prefix), StrLiteral(suffix))
  def addLinePrefixAndSuffix(prefix: Span, suffix: Span): T
}

trait Text extends Prefixable[Text] {
  def rawWidth(): Int
  def render(maxLength: Int = Int.MaxValue): String = render(0, maxLength)

  def render(minLength: Int, maxLength: Int): String = {
    // rewrite blocks
    val blocksRewritten = this match {
      case tb: TextBlock => tb.rewriteBlocks(maxLength)
      case t             => t
    }
    // rewrite to fixed length
    val fixedLength = blocksRewritten.rewriteToFixLength(minLength, maxLength)
    // rewrite to string
    fixedLength.rewriteToString()
  }
  def rewriteToFixLength(minLength: Int, maxLength: Int): Text
  def rewriteToString(): String
}

trait FixedLength

trait TextBlock extends Text with Prefixable[TextBlock] {
  def rewriteBlocks(maxLength: Int): TextBlock
  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): TextBlock
}

case class Block(texts: Text*) extends TextBlock {
  override def rawWidth(): Int = texts.map(_.rawWidth()).maxOption.getOrElse(0)

  override def rewriteBlocks(maxLength: Int): TextBlock = {
    val rewritenTexts = texts.map {
      case tb: TextBlock => tb.rewriteBlocks(maxLength)
      case t             => t
    }
    if (rewritenTexts.isEmpty) {
      EpsilonBlock
    } else {
      Block(rewritenTexts: _*)
    }
  }

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Text = {
    val lineLength = rawWidth()
    val thisMinLength = Math.max(lineLength, minLength)
    Block(texts.map(_.rewriteToFixLength(thisMinLength, maxLength)): _*)
  }

  override def rewriteToString(): String =
    texts.filterNot(_.isInstanceOf[Epsilon]).map(_.rewriteToString()).mkString(System.lineSeparator())

  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): Block =
    Block(texts.filterNot(_.isInstanceOf[Epsilon]).map(_.addLinePrefixAndSuffix(prefix, suffix)): _*)
}

case class PrefixSuffixBlock(prefix: Span, block: TextBlock, suffix: Span) extends TextBlock {
  override def rawWidth(): Int = prefix.rawWidth() + block.rawWidth() + suffix.rawWidth()
  override def rewriteBlocks(maxLength: Int): TextBlock = block.rewriteBlocks(maxLength)

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Text = {
    block.rewriteToFixLength(
      minLength - prefix.rawWidth() - suffix.rawWidth(),
      maxLength
    ).addLinePrefixAndSuffix(prefix, suffix)
  }
  override def rewriteToString(): String = block.rewriteToString()

  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): PrefixSuffixBlock =
    copy(prefix = this.prefix.addLinePrefix(prefix), suffix = this.suffix.addLineSuffix(suffix))
}

case class FillUpBlock(block: TextBlock, lineLength: Int, filler: Span) extends TextBlock {
  override def rawWidth(): Int = block.rawWidth()
  override def rewriteBlocks(maxLength: Int): TextBlock = block.rewriteBlocks(maxLength)

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Text = {
    val thisMinLength = Math.max(lineLength, minLength)
    val fixLengthBlock = block.rewriteToFixLength(thisMinLength, maxLength)
    val withFiller = fixLengthBlock.addLinePrefixAndSuffix(EpsilonSpan, filler)
    withFiller.rewriteToFixLength(thisMinLength, maxLength)
  }
  override def rewriteToString(): String = block.rewriteToString()

  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): FillUpBlock =
    copy(block = block.addLinePrefixAndSuffix(prefix, suffix))
}

/*
 * Note that we assume the
 * - all left are of the same length
 * - all right are of the same length
 */
trait BoxedBlockStyle {
  def topLeft: String
  def top: String
  def topRight: String
  def left: String
  def right: String
  def middleLeft: String
  def middle: String
  def middleRight: String
  def bottomLeft: String
  def bottom: String
  def bottomRight: String
}

case class SimpleBoxedBlockStyle(
  topLeft: String,
  top: String,
  topRight: String,
  left: String,
  right: String,
  middleLeft: String,
  middle: String,
  middleRight: String,
  bottomLeft: String,
  bottom: String,
  bottomRight: String
) extends BoxedBlockStyle

object BoxedBlockStyle {

  val thinLineBoxedBlock: BoxedBlockStyle = SimpleBoxedBlockStyle(
    topLeft = "┌",
    top = "─",
    topRight = "┐",
    left = "│",
    right = "│",
    middleLeft = "├",
    middle = "─",
    middleRight = "┤",
    bottomLeft = "└",
    bottom = "─",
    bottomRight = "┘"
  )
}

sealed trait BoxedBlockPositionInSequence {
  def hasBottom: Boolean
  def topLeft(boxedBlockStyle: BoxedBlockStyle): String
  def top(boxedBlockStyle: BoxedBlockStyle): String
  def topRight(boxedBlockStyle: BoxedBlockStyle): String
  def bottomLeft(boxedBlockStyle: BoxedBlockStyle): String = ""
  def bottom(boxedBlockStyle: BoxedBlockStyle): String = ""
  def bottomRight(boxedBlockStyle: BoxedBlockStyle): String = ""
}

object BoxedBlockPositionInSequence {

  case object First extends BoxedBlockPositionInSequence {
    override def hasBottom: Boolean = false
    override def topLeft(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.topLeft
    override def top(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.top
    override def topRight(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.topRight
  }

  case object Middle extends BoxedBlockPositionInSequence {
    override def hasBottom: Boolean = false
    override def topLeft(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.middleLeft
    override def top(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.middle
    override def topRight(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.middleRight
  }

  case object Last extends BoxedBlockPositionInSequence {
    override def hasBottom: Boolean = true
    override def topLeft(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.middleLeft
    override def top(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.middle
    override def topRight(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.middleRight
    override def bottomLeft(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.bottomLeft
    override def bottom(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.bottom
    override def bottomRight(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.bottomRight
  }

  case object Standalone extends BoxedBlockPositionInSequence {
    override def hasBottom: Boolean = true
    override def topLeft(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.topLeft
    override def top(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.top
    override def topRight(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.topRight
    override def bottomLeft(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.bottomLeft
    override def bottom(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.bottom
    override def bottomRight(boxedBlockStyle: BoxedBlockStyle): String = boxedBlockStyle.bottomRight
  }
}

case class BoxedBlock(
  positionInSequence: BoxedBlockPositionInSequence,
  boxedBlockStyle: BoxedBlockStyle,
  block: TextBlock
) extends TextBlock {

  override def rawWidth(): Int =
    boxedBlockStyle.left.length + block.rawWidth() + boxedBlockStyle.right.length

  override def rewriteBlocks(maxLength: Int): Block = {
    val preRenderedBlock = block.rewriteBlocks(maxLength)
    val lineLength = Math.min(maxLength, preRenderedBlock.rawWidth())
    val topLeft = StrLiteral(positionInSequence.topLeft(boxedBlockStyle))
    val top = positionInSequence.top(boxedBlockStyle)
    val topRight = StrLiteral(positionInSequence.topRight(boxedBlockStyle))
    val bottomLeft = StrLiteral(positionInSequence.bottomLeft(boxedBlockStyle))
    val bottom = positionInSequence.bottom(boxedBlockStyle)
    val bottomRight = StrLiteral(positionInSequence.bottomRight(boxedBlockStyle))
    val left = StrLiteral(boxedBlockStyle.left)
    val right = StrLiteral(boxedBlockStyle.right)
    Block(
      PrefixSuffixBlock(
        topLeft,
        FillUpBlock(Block(StrLiteral(top * lineLength)), lineLength, Span.filler(top)),
        topRight
      ),
      PrefixSuffixBlock(left, FillUpBlock(preRenderedBlock, lineLength, Span.filler()), right),
      if (positionInSequence.hasBottom) {
        PrefixSuffixBlock(
          bottomLeft,
          FillUpBlock(Block(StrLiteral(bottom * lineLength)), lineLength, Span.filler(bottom)),
          bottomRight
        )
      } else {
        EpsilonBlock
      }
    )
  }

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Text =
    block.rewriteToFixLength(minLength - boxedBlockStyle.left.length - boxedBlockStyle.right.length, maxLength)

  override def rewriteToString(): String = block.rewriteToString()

  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): BoxedBlock =
    copy(block = block.addLinePrefixAndSuffix(prefix, suffix))
}

object BoxedBlock {
  val defaultPositionInSequence: BoxedBlockPositionInSequence = Standalone
  val defaultBoxedBlockStyle: BoxedBlockStyle = BoxedBlockStyle.thinLineBoxedBlock

  def apply(
    positionInSequence: BoxedBlockPositionInSequence,
    boxedBlockStyle: BoxedBlockStyle,
    texts: Text*
  ): BoxedBlock =
    BoxedBlock(positionInSequence, boxedBlockStyle, Block(texts: _*))

  def apply(positionInSequence: BoxedBlockPositionInSequence, texts: Text*): BoxedBlock =
    BoxedBlock(positionInSequence, defaultBoxedBlockStyle, Block(texts: _*))

  def apply(texts: Text*): BoxedBlock =
    BoxedBlock(defaultPositionInSequence, defaultBoxedBlockStyle, Block(texts: _*))
}

case class IndentedBlock(indent: String, block: Block) extends TextBlock {

  override def rawWidth(): Int = block.rawWidth() + indent.length

  override def rewriteBlocks(maxLength: Int): TextBlock = Block(block.rewriteBlocks(maxLength).addLinePrefix(indent))

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Text =
    block.rewriteToFixLength(minLength - indent.length, maxLength)

  override def rewriteToString(): String = block.rewriteToString()

  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): IndentedBlock =
    copy(block = block.addLinePrefixAndSuffix(prefix, suffix))
}

object IndentedBlock {
  val defaultIndent: String = "  "
  def apply(texts: Text*): IndentedBlock = IndentedBlock(defaultIndent, Block(texts: _*))
}

sealed trait Span extends Text with Prefixable[Span] {

  override def rawWidth(): Int
  def literalSpans(): Seq[LiteralSpan]

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Span

  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): Span =
    Span((prefix.literalSpans() ++ literalSpans() ++ suffix.literalSpans()): _*)
}

object Span {
  def apply(line: String): SeqSpan = SeqSpan(Seq(StrLiteral(line)))
  def apply(parts: Span*): SeqSpan = SeqSpan(parts)
  def filler(fill: String = " "): LiteralSpan = FillerLiteral(fill)
  def shrinkAtBegin(part: String): LiteralSpan = ShrinkingLiteral(part, ShrinkAtBegin())
  def shrinkInMiddle(part: String): LiteralSpan = ShrinkingLiteral(part, ShrinkInMiddle())
  def shrinkAtEnd(part: String): LiteralSpan = ShrinkingLiteral(part, ShrinkAtEnd())
}

case class SeqSpan(parts: Seq[Span]) extends Span {
  private lazy val computedRawWidth: Int = parts.map(_.rawWidth()).sum
  override def rawWidth(): Int = computedRawWidth

  override def literalSpans(): Seq[LiteralSpan] = parts.flatMap(_.literalSpans())

  override def rewriteToFixLength(minLength: Int, maxLength: Int): SeqSpan = {
    if (rawWidth() < minLength) {
      val parts = literalSpans()
      // fill where fillable
      val (fixedLength, fillers) = parts.foldLeft((0, Seq.empty[FillerLiteral])) {
        case ((fixedLength, fillers), lp: StrLiteral)       => (fixedLength + lp.rawWidth(), fillers)
        case ((fixedLength, fillers), lp: FillerLiteral)    => (fixedLength, fillers :+ lp)
        case ((fixedLength, fillers), lp: ShrinkingLiteral) => (fixedLength + lp.rawWidth(), fillers)
      }
      if (fillers.nonEmpty) {
        val toFill = minLength - fixedLength
        val minLengthsCoarse = fillers.map(_ => Math.floorDiv(toFill, fillers.size))
        val remaining = toFill % fillers.size
        val minLengths: Map[LiteralSpan, Int] = fillers.zip(
          if (remaining == 0) {
            minLengthsCoarse
          } else {
            minLengthsCoarse.take(remaining).map(_ + 1) ++ minLengthsCoarse.drop(remaining)
          }
        ).toMap
        SeqSpan(parts.map(p => p.rewriteToFixLength(minLengths.getOrElse(p, Int.MaxValue), maxLength)))
      } else {
        this
      }
    } else if (maxLength < rawWidth()) {
      // shrink where shrinkable
      val parts = literalSpans()
      val (flexLength, flexParts) = parts.foldLeft((0, Seq.empty[LiteralSpan])) {
        case ((flexLength, flexParts), _: StrLiteral)        => (flexLength, flexParts)
        case ((flexLength, flexParts), _: FillerLiteral)     => (flexLength, flexParts)
        case ((flexLength, flexParts), lp: ShrinkingLiteral) => (flexLength + lp.rawWidth(), flexParts :+ lp)
      }
      val ratios = flexParts.map(p => p -> Math.floorDiv(flexLength, p.rawWidth())).toMap
      SeqSpan(parts.map(p => p.rewriteToFixLength(0, ratios.getOrElse(p, Int.MaxValue))))
    } else {
      // remove filler
      SeqSpan(parts.filterNot(_.isInstanceOf[FillerLiteral]))
    }
  }

  override def rewriteToString(): String = parts.map(_.rewriteToString()).mkString
}

sealed trait LiteralSpan extends Span {
  def part: String
  override def literalSpans(): Seq[LiteralSpan] = Seq(this)
  override def rewriteToString(): String = part
}

case class StrLiteral(part: String) extends LiteralSpan {
  override def rawWidth(): Int = part.length
  override def rewriteToFixLength(minLength: Int, maxLength: Int): Span = this
}

case class FillerLiteral(part: String) extends LiteralSpan {
  override def rawWidth(): Int = 0

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Span = {
    val fillerLength = part.length
    if (fillerLength < minLength) {
      StrLiteral(part * Math.floorDiv(minLength, fillerLength) + part.take(minLength % fillerLength))
    } else {
      EpsilonSpan
    }
  }
}

case class ShrinkingLiteral(part: String, shrinkingMode: ShrinkingMode) extends LiteralSpan {
  override def rawWidth(): Int = part.length

  override def rewriteToFixLength(minLength: Int, maxLength: Int): Span =
    StrLiteral(shrinkingMode.shrink(part, maxLength))
}

sealed trait ShrinkingMode {
  def shrinkingMarker: String

  def shrink(str: String, maxLength: Int): String = {
    if (str.length <= maxLength) {
      str
    } else {
      val markerLength = shrinkingMarker.length
      val available = maxLength - markerLength
      shrinkToAvailable(str, available)
    }
  }

  protected def shrinkToAvailable(str: String, available: Int): String
}

object ShrinkingMode {

  case class ShrinkAtBegin(shrinkingMarker: String = "… ") extends ShrinkingMode {

    override protected def shrinkToAvailable(str: String, available: Int): String =
      shrinkingMarker + str.take(available)
  }

  case class ShrinkInMiddle(shrinkingMarker: String = " … ") extends ShrinkingMode {

    override protected def shrinkToAvailable(str: String, available: Int): String = {
      val availableLeft = Math.ceilDiv(available, 2)
      val availableRight = Math.floorDiv(available, 2)
      str.take(availableLeft) + shrinkingMarker + str.takeRight(availableRight)
    }
  }

  case class ShrinkAtEnd(shrinkingMarker: String = " …") extends ShrinkingMode {

    override protected def shrinkToAvailable(str: String, available: Int): String =
      str.take(available) + shrinkingMarker
  }
}

trait Epsilon extends Text

case object EpsilonBlock extends Epsilon with TextBlock {
  override def rawWidth(): Int = 0
  override def rewriteToFixLength(minLength: Int, maxLength: Int): Text = this
  override def rewriteBlocks(maxLength: Int): TextBlock = this
  override def rewriteToString(): String = ""
  override def addLinePrefixAndSuffix(prefix: Span, suffix: Span): TextBlock = this
}

case object EpsilonSpan extends Epsilon with Span {
  override def rawWidth(): Int = 0
  override def literalSpans(): Seq[LiteralSpan] = Seq.empty
  override def rewriteToFixLength(minLength: Int, maxLength: Int): Span = this
  override def rewriteToString(): String = ""
}
