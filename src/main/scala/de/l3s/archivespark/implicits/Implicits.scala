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

package de.l3s.archivespark.implicits

import de.l3s.archivespark.cdx.{CdxRecord, ResolvedCdxRecord}
import de.l3s.archivespark.{ArchiveRecord, ResolvedArchiveRecord}

object Implicits extends Implicits

class Implicits {
  implicit def resolvedArchiveRecordToResolvedCdxRecord(record: ResolvedArchiveRecord): ResolvedCdxRecord = record.get
  implicit def archiveRecordToCdxRecord(record: ArchiveRecord): CdxRecord = record.get
}
