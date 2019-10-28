package com.presisco.pedia2neo4j.weibo

import java.util.concurrent.ConcurrentHashMap

object WeiboManager {

    val infoMap = ConcurrentHashMap<String, ConcurrentHashMap<String, Map<String, *>>>()

    val listMap = ConcurrentHashMap<String, ConcurrentHashMap<String, List<*>>>()

    fun setInfo(type: String, key: String, info: Map<String, *>) {
        if (!infoMap.containsKey(type)) {
            infoMap[type] = ConcurrentHashMap()
        }
        infoMap[type]!![key] = info
    }

    fun setList(type: String, key: String, list: List<*>) {
        if (!listMap.containsKey(type)) {
            listMap[type] = ConcurrentHashMap()
        }
        listMap[type]!![key] = list
    }

    fun getInfoValues(type: String) = infoMap[type]!!.values

    fun getFlattenList(type: String, keyName: String, valueName: String): List<Map<String, *>> {
        val flatten = arrayListOf<Map<String, *>>()
        listMap[type]!!.forEach { mid: String, values: List<*> ->
            values.forEach { value ->
                flatten.add(
                    hashMapOf(
                        keyName to mid,
                        valueName to value
                    )
                )
            }
        }
        return flatten
    }

    fun reset() {
        infoMap.clear()
        listMap.clear()
    }

}