package com.ynu.entity

/**
 * Created by spark on 15-1-16.
 */
class WorkingPara() {

  var taap = new Array[Int](256)
  var aap_begin = new Array[Int](256)
  var aap_list = new Array[Int](4000)
  var diag_score = new Array[Int](4000)

  def init() = {
    for (i <- 0 until 256) {
      this.taap(i) = 0
      this.aap_begin(i) = 0
    }

    for (i <- 0 until 4000) {
      this.aap_list(i) = 0
      this.diag_score(i) = 0
    }

  }


}