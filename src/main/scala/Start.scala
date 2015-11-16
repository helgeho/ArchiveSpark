/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 */

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.implicits._
import org.apache.spark._

object Start {
  def main(args: Array[String]): Unit = {
    val appName = "ArchiveSpark"
    val master = "yarn-client"

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    implicit val sc = new SparkContext(conf)

    val rdd = ArchiveSpark.hdfs("/data/ia/derivatives/de/cdx_orig_WILL_BE_REMOVED/TA/TA-100000-000000.arc.cdx", "/data/ia/w/de")
    val filteredRdd = rdd.filter(r => r.surtUrl.startsWith("de,entspannungs-shop"))
    val enriched = filteredRdd.enrich(StringContent)

//    val mapEnriched = enriched.mapEnrich[Array[Byte], String]("record.reponse.payload", "string") { payload =>
//      payload.toString
//    }

//    val dependencyMapEnriched = mapEnriched.mapEnrich[String, String](StringContent, "firstToken") { str =>
//      str.split(' ').head
//    }

    enriched.saveAsJson("out.json")
  }
}
