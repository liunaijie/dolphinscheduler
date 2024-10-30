/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.seatunnel;

import static org.apache.dolphinscheduler.common.constants.Constants.SEATUNNEL_LOCAL_PROPERTIES_PATH;
import static org.apache.dolphinscheduler.common.constants.Constants.SEATUNNEL_SERVER_URL;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.impl.Parseable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

@Slf4j
public class SeatunnelTask extends AbstractTask {

    private static final String SUBMIT_PATH = "/hazelcast/rest/maps/submit-job";
    private static final String RUNNING_JOBS = "/hazelcast/rest/maps/running-jobs";
    private static final String CANCEL_JOB = "/hazelcast/rest/maps/stop-job";
    private static final String GET_JOB = "/hazelcast/rest/maps/running-job/";
    private static final String FINISHED_JOB = "/hazelcast/rest/maps/finished-jobs/";

    private static final String JOB_STATUS_INITIALIZING = "INITIALIZING";
    private static final String JOB_STATUS_CREATED = "CREATED";
    private static final String JOB_STATUS_SCHEDULED = "SCHEDULED";
    private static final String JOB_STATUS_RUNNING = "RUNNING";
    private static final String JOB_STATUS_FAILING = "FAILING";
    private static final String JOB_STATUS_DOING_SAVEPOINT = "SAVEPOINT";
    private static final String JOB_STATUS_CANCELING = "CANCELING";
    private static final String JOB_STATUS_FAILED = "FAILED";
    private static final String JOB_STATUS_SAVEPOINT_DONE = "SAVEPOINT_DONE";
    private static final String JOB_STATUS_CANCELED = "CANCELED";
    private static final String JOB_STATUS_FINISHED = "FINISHED";
    private static final String JOB_STATUS_UNKNOWABLE = "UNKNOWABLE";

    public static final ConfigRenderOptions CONFIG_RENDER_OPTIONS = ConfigRenderOptions.concise().setFormatted(true);

    /**
     * seatunnel parameters
     */
    private SeatunnelParameters seatunnelParameters;

    /**
     * taskExecutionContext
     */
    protected final TaskExecutionContext taskExecutionContext;

    private final CloseableHttpClient httpClient;

    private final String serverUrl;
    private final String localPropertiesPath;
    private String jobId;

    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    public SeatunnelTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
        this.httpClient = createHttpClient();
        serverUrl = PropertyUtils.getString(SEATUNNEL_SERVER_URL);
        if (StringUtils.isEmpty(serverUrl)) {
            throw new TaskException("SeaTunnel server url is not configured");
        }
        localPropertiesPath = PropertyUtils.getString(SEATUNNEL_LOCAL_PROPERTIES_PATH);
    }

    @Override
    public void init() {
        log.info("Intialize SeaTunnel task params {}", JSONUtils.toPrettyJsonString(seatunnelParameters));
        if (seatunnelParameters == null || !seatunnelParameters.checkParameters()) {
            throw new TaskException("SeaTunnel task params is not valid");
        }
    }

    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        try {
            // generate config file to local
            String configFilePath = generateConfigFile();
            // replace config's variable with local config and parameters
            String jsonConfig = replaceConfigVariables(configFilePath);
            jobId = submitJob(jsonConfig);
            // sleep 10 seconds to wait for job running
            TimeUnit.SECONDS.sleep(10);
            // start to track job status
            trackJobStatus();
            setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("The current SeaTunnel task has been interrupted", e);
            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            throw new TaskException("The current SeaTunnel task has been interrupted", e);
        } catch (IOException e) {
            log.error("SeaTunnel task error", e);
            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            throw new TaskException("Execute Seatunnel task failed", e);
        }
    }

    @Override
    public void cancel() throws TaskException {
        if (jobId != null) {
            cancelJob();
        }
    }

    private String submitJob(String jsonConfig) throws IOException {
        log.info("Submitted config is \n{}\n", jsonConfig);
        RequestBuilder builder = RequestBuilder
                .post()
                .setEntity(new StringEntity(jsonConfig,
                        ContentType.create(ContentType.APPLICATION_JSON.getMimeType(), StandardCharsets.UTF_8)));
        String requestUrl =
                String.format("%s%s?jobName=%s", serverUrl, SUBMIT_PATH, taskExecutionContext.getTaskName());
        HttpUriRequest request = builder.setUri(requestUrl).build();
        CloseableHttpResponse httpResponse = httpClient.execute(request);
        JsonObject responseBody = getResponseBody(httpResponse);
        if (responseBody == null) {
            throw new TaskException("Submit SeaTunnel job failed");
        }
        if (responseBody.has("status") && responseBody.get("status").getAsString().equals("fail")) {
            throw new TaskException(responseBody.get("message").getAsString());
        }
        return responseBody.get("jobId").getAsString();
    }

    private void trackJobStatus() throws InterruptedException {
        while (true) {
            JsonObject jobStatus = checkJobStatus();
            if (jobStatus == null) {
                break;
            }
            String status = jobStatus.get("jobStatus").getAsString();
            if (status == null) {
                break;
            }
            JsonObject metrics = jobStatus.getAsJsonObject("metrics");
            String sourceReceivedCount = metrics.get("SourceReceivedCount").getAsString();
            String sinkWriteCount = metrics.get("SinkWriteCount").getAsString();
            log.info("the job status is [{}], Metrics: source received count: [{}], sink write count: [{}]", status,
                    sourceReceivedCount, sinkWriteCount);
            if (JOB_STATUS_FAILING.equals(status) || JOB_STATUS_FAILED.equals(status)) {
                throw new RuntimeException(jobStatus.get("errorMsg").getAsString());
            }
            if (JOB_STATUS_CANCELING.equals(status) || JOB_STATUS_CANCELED.equals(status)) {
                throw new RuntimeException("The job has is canceling or canceled");
            }
            if (Arrays.asList(JOB_STATUS_INITIALIZING, JOB_STATUS_CREATED, JOB_STATUS_SCHEDULED, JOB_STATUS_RUNNING,
                    JOB_STATUS_DOING_SAVEPOINT).contains(status)) {
                TimeUnit.SECONDS.sleep(5);
            } else {
                break;
            }
        }
    }

    private JsonObject checkJobStatus() {
        String requestUrl = String.format("%s%s%s", serverUrl, GET_JOB, jobId);
        HttpUriRequest request = RequestBuilder.get().setUri(requestUrl).build();
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(request);
            return getResponseBody(httpResponse);
        } catch (IOException e) {
            log.error("Check SeaTunnel job status failed", e);
            return null;
        }
    }

    private void cancelJob() throws TaskException {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("jobId", jobId);
        jsonObject.addProperty("isStopWithSavePoint", false);
        RequestBuilder builder = RequestBuilder
                .post()
                .setEntity(new StringEntity(jsonObject.toString(),
                        ContentType.create(ContentType.APPLICATION_JSON.getMimeType(), StandardCharsets.UTF_8)));
        String requestUrl = String.format("%s%s?jobName=%s", serverUrl, CANCEL_JOB, taskExecutionContext.getTaskName());
        HttpUriRequest request = builder.setUri(requestUrl).build();
        try {
            httpClient.execute(request);
        } catch (IOException e) {
            throw new TaskException("Cancel SeaTunnel task failed", e);
        }
    }

    protected CloseableHttpClient createHttpClient() {
        RequestConfig config = RequestConfig.custom().setSocketTimeout(120000)
                .setConnectTimeout(120000).build();
        HttpClientBuilder httpClientBuilder;
        httpClientBuilder = HttpClients.custom().setDefaultRequestConfig(config);
        return httpClientBuilder.build();
    }

    protected JsonObject getResponseBody(CloseableHttpResponse httpResponse) throws ParseException, IOException {
        if (httpResponse == null) {
            return null;
        }
        HttpEntity entity = httpResponse.getEntity();
        if (entity == null) {
            return null;
        }
        return new Gson().fromJson(EntityUtils.toString(entity, StandardCharsets.UTF_8.name()), JsonObject.class);
    }

    protected String generateConfigFile() throws IOException {
        String scriptContent;
        if (BooleanUtils.isTrue(seatunnelParameters.getUseCustom())) {
            scriptContent = buildCustomConfigContent();
        } else {
            String resourceFileName = seatunnelParameters.getResourceList().get(0).getResourceName();
            scriptContent = FileUtils.readFileToString(
                    new File(String.format("%s/%s", taskExecutionContext.getExecutePath(), resourceFileName)),
                    StandardCharsets.UTF_8);
        }
        String filePath = buildConfigFilePath();
        createConfigFileIfNotExists(scriptContent, filePath);
        return filePath;
    }

    private String replaceConfigVariables(String configFilePath) throws IOException {
        // read and parse config
        Config config = ConfigFactory.parseFile(new File(configFilePath));
        // resolve with global properties
        File localPropertiesFile = new File(localPropertiesPath);
        if (localPropertiesFile.exists()) {
            log.info("Start to resolve config with local properties file: {}", localPropertiesFile);
            JsonObject json = new Gson().fromJson(
                    FileUtils.readFileToString(localPropertiesFile, StandardCharsets.UTF_8), JsonObject.class);
            Map<String, String> configMap = new HashMap<>();
            for (String key : json.keySet()) {
                configMap.put(key, json.get(key).getAsString());
            }
            config = resolveWithMap(configMap, config);
        }
        // resolve with task parameters
        Map<String, String> definedParamsMap = getTaskParameters();
        config = resolveWithMap(definedParamsMap, config);
        if (!config.isResolved()) {
            throw new TaskException(String.format("There has some variable not resolved in config, " +
                    "Please confirm the variable added in the task parameter or added in the local properties file.\n" +
                    "Current config is {%s}", config.root().render(CONFIG_RENDER_OPTIONS)));
        }
        return config.root().render(CONFIG_RENDER_OPTIONS);
    }

    private Map<String, String> getTaskParameters() {
        Map<String, String> variables = new ConcurrentHashMap<>();
        Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();
        List<Property> propertyList = JSONUtils.toList(taskExecutionContext.getGlobalParams(), Property.class);
        if (propertyList != null && !propertyList.isEmpty()) {
            for (Property property : propertyList) {
                variables.put(property.getProp(), paramsMap.get(property.getProp()).getValue());
            }
        }
        List<Property> localParams = this.seatunnelParameters.getLocalParams();
        if (localParams == null || localParams.isEmpty()) {
            return variables;
        }
        for (Property property : localParams) {
            variables.put(property.getProp(), paramsMap.get(property.getProp()).getValue());
        }
        return variables;
    }

    private static Config resolveWithMap(Map<String, String> configMap, Config config) {
        configMap.forEach(System::setProperty);
        Config userConfig =
                Parseable.newProperties(
                        System.getProperties(),
                        ConfigParseOptions.defaults()
                                .setOriginDescription("system properties"))
                        .parse()
                        .toConfig();
        config =
                config.resolveWith(
                        userConfig,
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
        configMap.keySet().forEach(System::clearProperty);
        return config;
    }

    private String buildCustomConfigContent() {
        log.info("raw custom config content : {}", seatunnelParameters.getRawScript());
        String script = seatunnelParameters.getRawScript().replaceAll("\\r\\n", System.lineSeparator());
        script = parseScript(script);
        return script;
    }

    private String buildConfigFilePath() {
        return String.format("%s/seatunnel_%s.conf", taskExecutionContext.getExecutePath(),
                taskExecutionContext.getTaskAppId());
    }

    private void createConfigFileIfNotExists(String script, String scriptFile) throws IOException {
        log.info("tenantCode :{}, task dir:{}", taskExecutionContext.getTenantCode(),
                taskExecutionContext.getExecutePath());

        if (!Files.exists(Paths.get(scriptFile))) {
            log.info("generate script file:{}", scriptFile);

            // write data to file
            FileUtils.writeStringToFile(new File(scriptFile), script, StandardCharsets.UTF_8);
        }
    }

    @Override
    public AbstractParameters getParameters() {
        return seatunnelParameters;
    }

    private String parseScript(String script) {
        Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();
        return ParameterUtils.convertParameterPlaceholders(script, ParameterUtils.convert(paramsMap));
    }

    public void setSeatunnelParameters(SeatunnelParameters seatunnelParameters) {
        this.seatunnelParameters = seatunnelParameters;
    }
}
