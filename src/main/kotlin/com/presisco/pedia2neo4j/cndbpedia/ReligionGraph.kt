package com.presisco.pedia2neo4j.cndbpedia

import com.presisco.pedia2neo4j.mergeKeySets
import com.presisco.pedia2neo4j.nowMs

object ReligionGraph : AbstractSubjectGraph() {

    val partyRelations = setOf("教派", "学派")
    const val relatedPersonRelation = "主要人物"
    val aliasRelations = setOf("别名", "俗称", "别称", "又名")
    val belongsToRelations = setOf("所属宗教", "隶属")

    val recursiveSet = partyRelations.plus(aliasRelations).plus(belongsToRelations)

    val relationLabelsMap = hashMapOf(
        partyRelations to setOf("宗教"),
        belongsToRelations to setOf("宗教"),
        relatedPersonRelation to setOf("宗教人物")
    ).mergeKeySets()

    override fun targetLabelsForRelation(labels: Set<String>, relation: String): Set<String> =
        if (relationLabelsMap.containsKey(relation)) {
            relationLabelsMap.getValue(relation)
        } else if (aliasRelations.contains(relation)) {
            if (labels.contains("宗教")) {
                setOf("宗教")
            } else {
                setOf("人物")
            }
        } else {
            setOf("relation")
        }

    override fun isRecursive(labels: Set<String>, relation: String) = when (relation) {
        relatedPersonRelation -> true
        else -> recursiveSet.contains(relation)
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val start = nowMs()
        clearGraph()

        setOf("藏传佛教", "喇嘛", "达赖喇嘛").forEach(this::startWithMention)

        closeGraph()
        println("finished! takes ${(nowMs() - start) / 60000} minutes")
    }
}