"""
This example shows how to combine customers and orders.
"""

import apache_beam as beam
import sys


def split_order(line):
   order_data = line.split(",")
   order_id, price = order_data[0], order_data[5]
   yield (order_id,  float(price.strip(' "')))


if __name__ == '__main__':
   p = beam.Pipeline(argv=sys.argv)

   input = '../../brazilian-ecommerce/olist_order_items_dataset.csv'
   output_prefix = '/Users/sylvio/Downloads/dataflow_projects/tmp/output'
   
   # find all lines that contain the searchTerm
   (p
      | 'GetOrderItems' >> beam.io.ReadFromText(input, skip_header_lines=1)
      | 'SplitOrder' >> beam.FlatMap(split_order)
      | 'Sum elements' >> beam.CombinePerKey(sum)
      | 'Write' >> beam.io.WriteToText(output_prefix, file_name_suffix=".csv")
   )

   p.run().wait_until_finish()


