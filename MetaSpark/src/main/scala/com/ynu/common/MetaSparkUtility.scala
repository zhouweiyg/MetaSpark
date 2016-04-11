/*
 * MetaSparkUtility.scala for MetaSpark
 * Copyright (c) 2015-2016 Wei Zhou, Changchun Liu, Shuo Yuan All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
 
package com.ynu.common

/**
 * 工具类
 */
object MetaSparkUtility {

  val FILE_READ = "read"
  val FILE_REF  = "ref"
  
  val REDIS_READ_HS_PREFIX = "read_total_"
  val REDIS_REF_HS_PREFIX = "ref_total_"
  
  val REDIS_REFINDEX_HS_PREFIX = "refindex"
  
  val OPT_CREATE = "create"
  val OPT_DELETE = "delete"
  
  def getRedisHSName(fileType: String, fileName: String): String = {
    (fileType match {
      case FILE_READ => REDIS_READ_HS_PREFIX 
      case FILE_REF => REDIS_REF_HS_PREFIX
      case _ => ""
    }) + fileName
  }
  
  
}
