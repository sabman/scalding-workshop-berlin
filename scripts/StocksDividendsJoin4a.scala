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

/*
  ./run.rb ./scripts/StocksDividendsJoin4a.scala \
    --stocks data/stocks/AAPL.csv \
    --dividends data/dividends/AAPL.csv \
    --output output/AAPL-stocks-dividends-join.txt
 */

import com.twitter.scalding._
import workshop.Csv

class StocksDividendsJoin4a(args : Args) extends Job(args) {
  val stockSchema =
    ('symd, 'price_open, 'price_high, 'price_low,
      'price_close, 'volume, 'price_adj_close)
  val dividendsSchema = ('dymd, 'dividend)

  val stocksPipe = new Csv(args("stocks"), stockSchema)
    .read
    .project('symd, 'price_close)

  val dividendsPipe = new Csv(args("dividends"), dividendsSchema)
    .read

  stocksPipe
    .filter('symd){ ymd: String => ymd.startsWith("1988")}
    .joinWithTiny('symd -> 'dymd, dividendsPipe)
    .project('symd, 'price_close, 'dividend)
    .write(Tsv(args("output")))
}
