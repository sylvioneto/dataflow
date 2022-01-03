"""
This example shows how to filter delivered orders.
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
      | 'GetOrders' >> beam.io.ReadFromText(input)
      | 'Filter' >> beam.FlatMap(lambda line: my_grep(line, searchTerm) )
      | 'Write' >> beam.io.WriteToText(output_prefix)
   )

   p.run().wait_until_finish()
