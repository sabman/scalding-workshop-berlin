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

class WordCount2(args : Args) extends Job(args) {
  TextLine(args("input"))
    .read
    .flatMap('line -> 'word){
      line : String => tokenize(line)
    }
    .groupBy('word){ group => group.size('count) }
    .write(Tsv(args("output")))

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text
      .toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")
  }
}