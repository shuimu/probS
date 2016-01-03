/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object heatS {

  def main(args: Array[String]) {
    /*
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }
    */

    val args = new Array[String](3)
    args(0)="D:\\Users\\spark\\SparkTest\\input\\link.txt"
    args(1)="2"
    args(2)="D:\\Users\\spark\\SparkTest\\input\\node.txt"

    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    val iters = if (args.length > 1) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), 1)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }
    val cnt = links.groupByKey.map{ t =>
      (t._1, t._2.size)
    }
    //var ranks = links.mapValues(v => 1.0)
    // the init node score
    val nodes = ctx.textFile(args(2), 1)
    var ranks = nodes.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1).toDouble)
    }
    for (i <- 1 to iters) {
      ranks = links.join(cnt).map{ t =>
        (t._2._1, (t._1, t._2._2))
      }.join(ranks).map{ t =>
        (t._2._1._1, t._2._2/t._2._1._2)
      }.reduceByKey(_ + _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    ctx.stop()
  }
}
