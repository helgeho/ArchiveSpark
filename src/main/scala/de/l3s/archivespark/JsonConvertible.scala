package de.l3s.archivespark

import de.l3s.archivespark.utils.Json._

/**
 * Created by holzmann on 16.09.2015.
 */
trait JsonConvertible {
  def toJson: Map[String, Any]

  def toJsonString: String = mapToJson(toJson)
}
