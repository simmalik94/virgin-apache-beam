import apache_beam as beam
from src.transformations import Transform

with beam.Pipeline() as p1:
    data = (p1
            | beam.io.ReadFromText('transactions.csv', skip_header_lines=1)
            | 'composite transformations' >> Transform()
            | "output" >> beam.io.WriteToText
            (
                'output/results', header='date, total_amount', file_name_suffix='.json.gz'
            )
            )
