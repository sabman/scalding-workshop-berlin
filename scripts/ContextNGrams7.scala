/*
Copyright 2012 Think Big Analytics, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import com.twitter.scalding._

class ContextNGrams7(args : Args) extends Job(args) {

  val ngramPrefix = args.list("ngram-prefix").mkString(" ")
  val numberOfNGrams = args.getOrElse("count", "10").toInt

  val ngramRE = new scala.util.matching.Regex(ngramPrefix + """\s+(\S+)""")

  val lines = TextLine(args("input"))
    .read
    .flatMap('line -> 'ngram) {
      text: String => ngramRE.findAllIn(text).toIterable }
    .discard('num, 'line)
    .groupBy('ngram) { _.size('count) }
    .debug
    .write(Tsv(args("output")))
}