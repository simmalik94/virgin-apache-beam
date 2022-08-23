import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty

with TestPipeline() as p:
    output = (
            p
            | beam.io.ReadFromText('output/results-00000-of-00001.json')
    )

    assert_that(output, is_not_empty())
