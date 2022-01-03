#!/usr/bin/env python

"""
Copyright Google Inc. 2016
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import apache_beam as beam
import sys


def my_grep(line, term):
   if line.find(term) > 0:
      yield line


if __name__ == '__main__':
   p = beam.Pipeline(argv=sys.argv)
   input = '../../brazilian-ecommerce/olist_orders_dataset.csv'
   output_prefix = '/tmp/output'
   searchTerm = 'delivered'

   # find all lines that contain the searchTerm
   (p
      | 'ReadOrders' >> beam.io.ReadFromText(input)
      | 'Grep' >> beam.FlatMap(lambda line: my_grep(line, searchTerm) )
      | 'Write' >> beam.io.WriteToText(output_prefix)
   )

   p.run().wait_until_finish()
