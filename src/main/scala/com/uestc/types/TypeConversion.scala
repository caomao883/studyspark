package com.uestc.types

object TypeConversion {
  def main(args: Array[String]): Unit = {
    val content = "#4652"
    println(bytes2String2(string2bytes(content)))
  }
  def bytes2String2(bytes : Array[Byte]): String ={
    return bytes.map("%02x".format(_)).mkString
  }
  def bytes2String(bytes : Array[Byte]): String ={
    new String(bytes)
  }
  def string2bytes(str: String): Array[Byte] ={
    str.getBytes()
  }
}
