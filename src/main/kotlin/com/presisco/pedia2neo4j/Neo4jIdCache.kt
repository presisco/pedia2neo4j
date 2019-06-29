package com.presisco.pedia2neo4j

object Neo4jIdCache {
    val entityList = arrayListOf<String>()
    val labelsList = arrayListOf<Set<String>>()
    val idList = arrayListOf<Int>()

    fun idFor(name: String, labels: Set<String>): Int {
        for (i in 0.until(entityList.size)) {
            if (entityList[i].contains(name) && labelsList[i].containsAll(labels)) {
                return idList[i]
            }
        }
        return -1
    }

    fun addIdFor(name: String, labels: Set<String>, id: Int) {
        entityList.add(name)
        labelsList.add(labels)
        idList.add(id)
    }

    fun updateLabels(name: String, labels: Set<String>) {
        for (i in 0.until(entityList.size)) {
            if (entityList[i] == name) {
                labelsList[i] = labels
            }
        }
    }

}