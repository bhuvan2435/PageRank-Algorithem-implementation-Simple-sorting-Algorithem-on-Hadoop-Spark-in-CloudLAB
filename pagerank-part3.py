import re         //This provides regular expression mathcing operation
import time       //Time related function like datetime-calendar
import sys        //Provide variable used which are maintained by interpreter
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pagerank <file>")
        sys.exit(-1)

    start_time = time.time()

    spark = SparkSession\
        .builder\
        .appName("CS 494 HW1 Part-3 PageRank by Sankul")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: (r[0],r[0])).partitionBy(2).persist()

    links = lines.map(lambda urls: parseNeighbors(urls[0])).distinct().groupByKey().cache()

    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    for iteration in range(10):
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    for (link, rank) in ranks.collect():
        print("%s has rank: %s" % (link, rank))

    spark.stop()

    print("Elapsed time = ", time.time() - start_time)
