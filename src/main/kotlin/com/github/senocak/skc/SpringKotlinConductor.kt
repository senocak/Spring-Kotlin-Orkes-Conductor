package com.github.senocak.skc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.netflix.conductor.client.http.MetadataClient
import com.netflix.conductor.client.http.TaskClient
import com.netflix.conductor.client.http.WorkflowClient
import com.netflix.conductor.client.worker.Worker
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest
import io.orkes.conductor.client.ApiClient
import io.orkes.conductor.client.OrkesClients
import io.orkes.conductor.client.automator.TaskRunnerConfigurer
import io.orkes.conductor.client.http.OrkesMetadataClient
import io.orkes.conductor.client.http.OrkesTaskClient
import io.orkes.conductor.client.http.OrkesWorkflowClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.event.EventListener
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.client.RestTemplate
import java.io.IOException

fun main(args: Array<String>) {
    runApplication<SpringKotlinConductor>(*args)
}

@SpringBootApplication
class SpringKotlinConductor(
    @Value("\${conductor.server.url}") val url: String,
    @Value("\${conductor.threadCount}") val threadCount: Int,
    @Value("\${conductor.timeOut:30000}") val timeOut: Int,
) {
    @EventListener(ApplicationReadyEvent::class)
    fun setup() {
        WorkflowDefinition.loadWorkflowsAndTasks(url = url)
    }

    @Bean
    fun orkesClients(): OrkesClients =
        ApiClient(url)
            .also {
                it.setWriteTimeout(timeOut)
                it.setReadTimeout(timeOut)
                it.setConnectTimeout(timeOut)
            }
            .run { OrkesClients(this) }

    @Bean
    fun apiClient(): ApiClient =
        ApiClient(url)
            .also {
                it.setWriteTimeout(timeOut)
                it.setReadTimeout(timeOut)
                it.setConnectTimeout(timeOut)
            }

    @Bean
    fun getTaskClient(apiClient: ApiClient, workerList: List<Worker>): TaskClient {
        //var taskClient = orkesClients().taskClient
        val orkesTaskClient = OrkesTaskClient(apiClient)
        val taskRunnerConfigurer = TaskRunnerConfigurer.Builder(orkesTaskClient, workerList)
                .withThreadCount(threadCount)
                .withWorkerNamePrefix("OrkesWorkerPref")
                .build()
        taskRunnerConfigurer.init()
        return orkesTaskClient
    }

    @Bean
    fun getWorkflowClient(apiClient: ApiClient): WorkflowClient =
        //var workflowClient = orkesClients().workflowClient
        OrkesWorkflowClient(apiClient)

    @Bean
    fun getMetadataClient(apiClient: ApiClient): MetadataClient =
        //var metadataClient = orkesClients().metadataClient
        OrkesMetadataClient(apiClient)
}

object WorkflowDefinition {
    private const val TASK_DEF_PATH = "/metadata/taskdefs"
    private const val WORKFLOW_DEF_PATH = "/metadata/workflow"
    private val objectMapper = ObjectMapper()
    private val workflows: MutableList<JsonNode> = ArrayList()
    private val restTemplate = RestTemplate()
    private val headers = HttpHeaders().also { it.contentType = MediaType.APPLICATION_JSON }

    @Throws(IOException::class)
    fun loadWorkflowsAndTasks(url: String) {
        val taskList = readTasks()
        val workflowTaskNames = readTaskNameFromWorkflow()
        checkTasksInWorkflow(taskList = taskList, workflowsTaskNames = workflowTaskNames)
        defineTasksInConductorServer(url = url, taskList = taskList)
        defineWorkflowsInConductorServer(url = url)
    }

    private fun defineWorkflowsInConductorServer(url: String) {
        val arrayNode = ArrayNode(JsonNodeFactory.instance)
        workflows.forEach { value: JsonNode? -> arrayNode.add(value) }
        restTemplate.put(url + WORKFLOW_DEF_PATH, HttpEntity(arrayNode, headers))
    }

    private fun defineTasksInConductorServer(url: String, taskList: MutableList<Task>) {
        val tasks = restTemplate.getForEntity(url + TASK_DEF_PATH, String::class.java)
        if (tasks.statusCode.is2xxSuccessful && tasks.body != null) {
            val conductorTasks = objectMapper.readTree(tasks.body) as ArrayNode
            conductorTasks.forEach { jsonNode: JsonNode ->
                val taskIterator = taskList.iterator()
                while (taskIterator.hasNext()) {
                    val task = taskIterator.next()
                    if (jsonNode["name"].textValue() == task.name) {
                        restTemplate.put(url + TASK_DEF_PATH, HttpEntity(task.jsonNode, headers))
                        taskIterator.remove()
                    }
                }
            }
            val arrayNode = ArrayNode(JsonNodeFactory.instance)
            taskList.forEach { task: Task -> arrayNode.add(task.jsonNode) }
            if (!arrayNode.isEmpty) {
                val httpEntity = HttpEntity(arrayNode, headers)
                //objectMapper.writeValueAsString(arrayNode)
                restTemplate.postForObject(url + TASK_DEF_PATH, httpEntity, Void::class.java)
            }
        }
    }

    private fun checkTasksInWorkflow(taskList: List<Task>, workflowsTaskNames: List<Task>) {
        workflowsTaskNames.forEach { workflowTask ->
            if (workflowTask.type == "SIMPLE" &&
                !workflowsTaskNames.any { wtn -> taskList.any { tl -> tl.name == wtn.name } }) {
                val message = "Workflow Task scan failed. Be careful about :$workflowTask"
                println(message = message)
                throw IllegalArgumentException(message)
            }
        }
    }

    private fun readTasks(): MutableList<Task> {
        val taskNames: MutableList<Task> = arrayListOf()
        val jsonNode = objectMapper.readTree(javaClass.classLoader.getResource("task.json")) as ArrayNode
        jsonNode.forEach { node: JsonNode ->
            taskNames.add(element = Task(name = node["name"].textValue(), type = "SIMPLE", jsonNode = node))
        }
        return taskNames
    }

    private fun readTaskNameFromWorkflow(): List<Task> {
        val arrayNode = objectMapper.readTree(javaClass.classLoader.getResource("workflow.json")) as ArrayNode
        val taskNames: MutableList<Task> = arrayListOf()
        arrayNode.elements().forEachRemaining { jsonNode: JsonNode ->
            workflows.add(element = jsonNode)
            val lArrayNode = jsonNode["tasks"] as ArrayNode
            lArrayNode.forEach { node: JsonNode ->
                taskNames.add(element = Task(name = node["name"].textValue(),
                    type = node["type"].textValue(), jsonNode = node))
            }
        }
        return taskNames
    }

    data class Task(var name: String, var type: String, var jsonNode: JsonNode)
}

@RestController
@RequestMapping("/api/v1")
class WorkflowTriggerController(
    private val workflowClient: WorkflowClient
) {
    @GetMapping("/helloworld/{name}")
    fun helloWorldTrigger(@PathVariable name: String): String =
        triggerWorkflowByNameAndInput(workflowName = "hello_world_workflow", input = mapOf("name" to name))

    @GetMapping("/movie/{movieType}/{movieId}")
    fun movieTrigger(@PathVariable movieType: String, @PathVariable movieId: String): String =
        triggerWorkflowByNameAndInput(workflowName = "decision_workflow",
            input = mapOf("movieType" to movieType, "movieId" to movieId))

    private fun triggerWorkflowByNameAndInput(workflowName: String, input: Map<String, Any?>): String =
        StartWorkflowRequest()
            .also {
                it.input = input
                it.withName(workflowName)
            }
            .run { workflowClient.startWorkflow(this) }
}
@Service
class HelloWorldWorker: Worker {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    override fun getTaskDefName(): String = "hello_world"
    override fun execute(task: Task): TaskResult {
        log.info("HELLO WORLD: ${task.inputData["name"]}")
        return TaskResult.complete()
    }
}
@Service
class GenerateEpisodeArtwork: Worker {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    override fun getTaskDefName(): String = "generate_episode_artwork"
    override fun execute(task: Task): TaskResult {
        val movieId = task.inputData["movieId"] as String?
        log.info("GENERATE EPISODE ARTWORK : Movie ID : $movieId")
        return TaskResult.complete()
    }
}
@Service
class GenerateMovieArtwork : Worker {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    override fun getTaskDefName(): String = "generate_movie_artwork"
    override fun execute(task: Task): TaskResult {
        val movieId = task.inputData["movieId"] as String?
        log.info("Generate Movie Artwork. Movie Id : $movieId")
        return TaskResult.complete()
    }
}
@Service
class SetupEpisodesWorker: Worker {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    override fun getTaskDefName(): String = "setup_episodes"
    override fun execute(task: Task): TaskResult {
        val movieId = task.inputData["movieId"] as String?
        log.info("Setup Episodes. Movie Id : $movieId")
        return TaskResult.complete()
    }
}
@Service
class SetupMovie: Worker {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    override fun getTaskDefName(): String = "setup_movie"
    override fun execute(task: Task): TaskResult {
        val movieId = task.inputData["movieId"] as String?
        log.info("Setup Movie. Movie Id : $movieId")
        return TaskResult.complete()
    }
}
