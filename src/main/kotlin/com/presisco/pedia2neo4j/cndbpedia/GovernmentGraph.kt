package com.presisco.pedia2neo4j.cndbpedia

import com.presisco.pedia2neo4j.nowMs

object GovernmentGraph : AbstractSubjectGraph() {

    override fun targetLabelsForRelation(labels: Set<String>, relation: String) = when (relation) {
        "机场", "火车站" -> setOf("交通设施")
        else -> setOf(relation)
    }

    override fun isRecursive(labels: Set<String>, relation: String) = setOf(
        "著名景点", "机场", "火车站", "现任领导", "现任市长"
    ).contains(relation)

    @JvmStatic
    fun main(args: Array<String>) {
        val start = nowMs()
        clearGraph()

        setOf("北京", "北京市政府").forEach(this::startWithMention)

        closeGraph()
        println("finished! takes ${(nowMs() - start) / 60000} minutes")
    }
}