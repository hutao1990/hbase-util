package com.util

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

/**
  * Created by hutao on 2017/4/11.
  */
object ConfigUtils {
  private val map = mutable.Map[String,ConfigUtils]()


  def get(file:String,key: String):String={
    getConfig(file).getKey(key)
  }

  def getInt(file:String,key: String):Int={
    get(file,key).toInt
  }

  def getFloat(file:String,key: String):Float={
    get(file,key).toFloat
  }

  def getDouble(file:String,key: String):Double={
    get(file,key).toDouble
  }

  def getBoolean(file:String,key: String):Boolean={
    get(file,key).toBoolean
  }

  /**
    * 在配置文件中找出key中包含一些字符串的k-v对
    * @param file 配置文件
    * @param likekey 匹配字符串
    * @return
    */
  def getMapByCondition(file:String,likekey:String):Map[String,String]={
    val confMap = getConfig(file).getMap
    val resultMap = mutable.Map[String,String]()
    for ((k,v)<-confMap if StringUtils.contains(k,likekey)){
      resultMap.put(k,v)
    }
    resultMap.toMap[String,String]
  }

  /**
    * 根据file获取对应的配置
    * @param file 配置文件
    * @return
    */
  def getConfig(file:String):ConfigUtils={
    if (!map.contains(file)){
      map.put(file,new ConfigUtils(file))
    }
    map(file)
  }
}

/**
  * 读取配置文件类
  * @param file
  */
class ConfigUtils(file:String){
  // 加载配置文件
  private val prop = new Properties()
  private val propFile = new File(file)
  if (propFile.exists()){
    prop.load(new FileInputStream(propFile))
  }else{
    prop.load(this.getClass.getResourceAsStream(file))
  }

  /**
    * 根据key获取value
    * @param key
    * @return
    */
  def getKey(key:String):String={
    prop.getProperty(key)
  }

  /**
    * 获取所有的k-v
    * @return
    */
  def getMap:Map[String,String]={
    var map = mutable.Map[String,String]()
    for (k <- prop.keySet().toArray(Array[String]())){
      map.put(k,prop.getProperty(k))
    }
    map.toMap[String,String]
  }
}