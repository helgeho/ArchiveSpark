package de.l3s.archivespark

import com.github.nscala_time.time.Imports._

/**
 * Created by holzmann on 04.08.2015.
 */
abstract class ArchiveRecord(cdx: CdxRecord) extends EnrichRoot[ArchiveRecord] {
  private var _url = new EnrichRootField[String, ArchiveRecord](this, new ArchiveRecordField(cdx.url), (root, field) => root._url = field)
  private var _time = new EnrichRootField[DateTime, ArchiveRecord](this, new ArchiveRecordField(cdx.time), (root, field) => root._time = field)
  private var _fullUrl = new EnrichRootField[String, ArchiveRecord](this, new ArchiveRecordField(cdx.fullUrl), (root, field) => root._fullUrl = field)
  private var _mimeType = new EnrichRootField[String, ArchiveRecord](this, new ArchiveRecordField(cdx.mimeType), (root, field) => root._mimeType = field)
  private var _status = new EnrichRootField[Int, ArchiveRecord](this, new ArchiveRecordField(cdx.status), (root, field) => root._status = field)
  private var _checksum = new EnrichRootField[String, ArchiveRecord](this, new ArchiveRecordField(cdx.checksum), (root, field) => root._checksum = field)
  private var _redirectUrl = new EnrichRootField[String, ArchiveRecord](this, new ArchiveRecordField(cdx.redirectUrl), (root, field) => root._redirectUrl = field)
  private var _meta = new EnrichRootField[String, ArchiveRecord](this, new ArchiveRecordField(cdx.meta), (root, field) => root._meta = field)

  val url = _url
  val time = _time
  val fullUrl = _fullUrl
  val mimeType = _mimeType
  val status = _status
  val checksum = _checksum
  val redirectUrl = _redirectUrl
  val meta = _meta

  protected def bytes: Array[Byte]

  private var _response: EnrichRootField[Array[Byte], ArchiveRecord] = null
  def hasResponse = _response != null
  def response: EnrichRootField[Array[Byte], ArchiveRecord] = {
    if (hasResponse) _response
    _response = new EnrichRootField[Array[Byte], ArchiveRecord](this, new ArchiveRecordField(bytes), (root, field) => root._response = field)
    _response
  }
}
