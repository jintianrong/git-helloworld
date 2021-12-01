package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
  * 读取配置文件工具类
  */
object PropertiesUtil {

  def load(propertieName:String):Properties = {
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName),"UTF-8"))
    prop
  }
}
