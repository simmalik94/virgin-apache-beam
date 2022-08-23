import apache_beam as beam


def select(my_data):
    return [my_data[0], my_data[3]]


def condition1(element):
    if int(float(element[1])) > 20:
        return element


def condition2(element):
    return element[0] > '2010-01-01'


def join(element):
    return ', '.join(element)


def format1(element):
    return [element[0][0:10], element[1]]


class Transform(beam.PTransform):
    def expand(self, pcol):
        a = (
                pcol | beam.Map(lambda line: line.split(","))
                | beam.Map(select)
                | "Filter1" >> beam.Filter(condition1)
                | "Filter2" >> beam.Filter(condition2)
                | beam.Map(format1)
                | beam.Map(join)
        )
        return a
