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