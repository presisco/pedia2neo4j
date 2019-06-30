package com.presisco.pedia2neo4j.cndbpedia

object CompanyGraph : AbstractSubjectGraph() {
    const val companyLabel = "公司"
    const val companyCreatorRelation = "创办人"
    const val companyProduceRelation = "开发商"
    const val companyPublisherRelation = "发行商"

    override fun targetLabelsForRelation(labels: Set<String>, relation: String) = when (relation) {
        companyCreatorRelation, "创始人" -> setOf("经济人物")
        companyProduceRelation, companyPublisherRelation, "主办单位", "开发公司" -> setOf(companyLabel)
        else -> setOf(relation)
    }

    override fun isRecursive(labels: Set<String>, relation: String) = when (relation) {
        companyCreatorRelation, "创始人",
        companyPublisherRelation, companyProduceRelation,
        "主办单位", "开发公司" -> true
        else -> false
    }

    @JvmStatic
    fun main(args: Array<String>) {
        clearGraph()

        setOf("搜狗", "搜狗输入法", "搜狐", "搜狗浏览器")
            .forEach(this::startWithMention)

        closeGraph()
        println("finished!")
    }
}