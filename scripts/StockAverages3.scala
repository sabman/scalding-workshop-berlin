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
import workshop.Csv

class StockAverages3(args : Args) extends Job(args) {

  val stockSchema =
    ('ymd, 'price_open, 'price_high, 'price_low,
      'price_close, 'volume, 'price_adj_close)

  new Csv(args("input"), stockSchema)
    .read
    .project('ymd, 'price_adj_close)
    .mapTo(('ymd, 'price_adj_close) -> ('year, 'closing_price)) {
      ymd_close: (String, String) =>
      (year(ymd_close._1), (ymd_close._2).toDouble)
    }
    .groupBy('year) {
      group => group.sizeAveStdev(
        'closing_price -> ('size, 'average_close, 'std_dev))
    }
    .write(Tsv(args("output")))

  def year(date: String): Int = Integer.parseInt(date.split("-")(0))
}
