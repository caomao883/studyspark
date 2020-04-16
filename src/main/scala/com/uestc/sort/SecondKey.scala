package com.uestc.sort

class SecondKey(val first: Int,val second: Int) extends Ordered[SecondKey] with Serializable{
  override def compare(that: SecondKey):Int= {
    if (this.first == that.first) {
      this.second - that.second
    } else {
      this.first - that.first
    }
  }
}
