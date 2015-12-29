cnt = pairs.groupByKey.map( t => (t._1, t._2.size)).collect
links.join(cnt).map( t => (t._1, t._2._1, t._2._2)).collect
links.join(cnt).join(ranks).map( t => (t._1, t._2._1._1, t._2._1._2, t._2._2)).collect
